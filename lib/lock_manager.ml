type permission =
  | ReadPerm
  | WritePerm

type page_lock =
  | SharedLock of int
  | ExclusiveLock

let page_lock_of_perm = function
  | ReadPerm -> SharedLock 1
  | WritePerm -> ExclusiveLock
;;

type lock_manager =
  | LockManger of
      { lock_mutex : Mutex.t
      ; page_locks : (Db_page.page_key, page_lock) Hashtbl.t
      ; tran_locks : (Transaction.tran_id, Db_page.PageKeySet.t) Hashtbl.t
      ; blocked_trans : (Db_page.page_key, Transaction.TranSet.t) Hashtbl.t
      }

let create () =
  LockManger
    { lock_mutex = Mutex.create ()
    ; page_locks = Hashtbl.create 16
    ; tran_locks = Hashtbl.create 16
    ; blocked_trans = Hashtbl.create 16
    }
;;

let get_locked_pages (LockManger lm) tid =
  match Hashtbl.find_opt lm.tran_locks tid with
  | Some locked_pages -> locked_pages
  | None -> failwith "internal error"
;;

let get_locked_pages_list lm tid = get_locked_pages lm tid |> Db_page.PageKeySet.to_list

let begin_transaction (LockManger lm) tid =
  Mutex.protect lm.lock_mutex (fun () ->
    Hashtbl.add lm.tran_locks tid Db_page.PageKeySet.empty)
;;

let extend_tran_locks (LockManger lm) tid page =
  let locked_pages =
    match Hashtbl.find_opt lm.tran_locks tid with
    | Some locked_pages -> Db_page.PageKeySet.add page locked_pages
    | None -> failwith "internal error"
  in
  Hashtbl.replace lm.tran_locks tid locked_pages
;;

let try_acquire_lock (LockManger lm) page tid perm =
  let locked_pages = get_locked_pages (LockManger lm) tid in
  match Hashtbl.find_opt lm.page_locks page, perm with
  | None, _ ->
    Hashtbl.add lm.page_locks page (page_lock_of_perm perm);
    extend_tran_locks (LockManger lm) tid page;
    true
  | Some ExclusiveLock, _ -> Db_page.PageKeySet.mem page locked_pages
  | Some (SharedLock num_trans), ReadPerm ->
    if not (Db_page.PageKeySet.mem page locked_pages)
    then (
      Hashtbl.replace lm.page_locks page (SharedLock (num_trans + 1));
      extend_tran_locks (LockManger lm) tid page);
    true
  | Some (SharedLock num_trans), WritePerm
    when num_trans = 1 && Db_page.PageKeySet.mem page locked_pages ->
    Hashtbl.replace lm.page_locks page ExclusiveLock;
    true
  | Some (SharedLock num_trans), WritePerm -> false
;;

let get_blocked_trans (LockManger lm) page =
  match Hashtbl.find_opt lm.blocked_trans page with
  | Some blocked -> blocked
  | None -> Transaction.TranSet.empty
;;

module TranGraph = Transaction.TranGraph
module TranGraphSCC = Graph.Components.Make (TranGraph)

let check_deadlock (LockManger lm) page tid =
  let g = TranGraph.create () in
  Hashtbl.iter
    (fun t1 locked_pages ->
      Db_page.PageKeySet.iter
        (fun locked_page ->
          Transaction.TranSet.iter
            (fun t2 ->
              if t1 <> t2
              then TranGraph.add_edge g (TranGraph.V.create t2) (TranGraph.V.create t1))
            (get_blocked_trans (LockManger lm) page))
        locked_pages)
    lm.tran_locks;
  let num_comps, _ = TranGraphSCC.scc g in
  let deadlock_free = num_comps = TranGraph.nb_vertex g in
  deadlock_free
;;

let block_tran (LockManger lm) page tid =
  let blocked_trans = get_blocked_trans (LockManger lm) page in
  Hashtbl.replace lm.blocked_trans page (Transaction.TranSet.add tid blocked_trans);
  if check_deadlock (LockManger lm) page tid
  then true
  else (
    Hashtbl.replace lm.blocked_trans page blocked_trans;
    false)
;;

let unblock_tran (LockManger lm) page tid =
  match Hashtbl.find_opt lm.blocked_trans page with
  | Some blocked ->
    Hashtbl.replace lm.blocked_trans page (Transaction.TranSet.remove tid blocked)
  | None -> ()
;;

let rec acquire_lock (LockManger lm) page tid perm abort_tran =
  Mutex.lock lm.lock_mutex;
  if try_acquire_lock (LockManger lm) page tid perm
  then (
    unblock_tran (LockManger lm) page tid;
    Mutex.unlock lm.lock_mutex)
  else (
    if not (block_tran (LockManger lm) page tid)
    then (
      abort_tran ();
      Mutex.unlock lm.lock_mutex;
      raise (Error.DBError Error.Deadlock_victim));
    Mutex.unlock lm.lock_mutex;
    Unix.sleepf 0.05;
    acquire_lock (LockManger lm) page tid perm abort_tran)
;;

let relase_locks (LockManger lm) tid =
  match Hashtbl.find_opt lm.tran_locks tid with
  | Some locked_pages ->
    Db_page.PageKeySet.iter
      (fun page ->
        match Hashtbl.find_opt lm.page_locks page with
        | Some ExclusiveLock | Some (SharedLock 1) -> Hashtbl.remove lm.page_locks page
        | Some (SharedLock num_trans) ->
          Hashtbl.replace lm.page_locks page (SharedLock (num_trans - 1))
        | None -> failwith "internal error")
      locked_pages;
    Hashtbl.remove lm.tran_locks tid
  | None -> ()
;;

let protect (LockManger lm) f = Mutex.protect lm.lock_mutex f
