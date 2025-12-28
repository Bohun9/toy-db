open Core
module PageKeySet = Set.Make (Page_key)
module TransactionIdSet = Set.Make (Transaction_id)
module TransactionIdGraph = Graph.Imperative.Digraph.Concrete (Transaction_id)
module TransactionIdGraphSCC = Graph.Components.Make (TransactionIdGraph)

type permission =
  | ReadPerm
  | WritePerm
[@@deriving show { with_path = false }]

type page_lock =
  | SharedLock of int
  | ExclusiveLock

let page_lock_of_perm = function
  | ReadPerm -> SharedLock 1
  | WritePerm -> ExclusiveLock
;;

type t =
  { lock_mutex : Mutex.t
  ; page_locks : (Page_key.t, page_lock) Hashtbl.t
  ; tran_locks : (Transaction_id.t, PageKeySet.t) Hashtbl.t
  ; blocked_trans : (Page_key.t, TransactionIdSet.t) Hashtbl.t
  }

let create () =
  { lock_mutex = Mutex.create ()
  ; page_locks = Hashtbl.create 16
  ; tran_locks = Hashtbl.create 16
  ; blocked_trans = Hashtbl.create 16
  }
;;

let get_locked_pages lm tid =
  match Hashtbl.find_opt lm.tran_locks tid with
  | Some locked_pages -> locked_pages
  | None -> failwith "internal error - get_locked_pages"
;;

let get_locked_pages_list lm tid = get_locked_pages lm tid |> PageKeySet.to_list

let begin_transaction lm tid =
  Mutex.protect lm.lock_mutex (fun () -> Hashtbl.add lm.tran_locks tid PageKeySet.empty)
;;

let extend_tran_locks lm tid page =
  let locked_pages =
    match Hashtbl.find_opt lm.tran_locks tid with
    | Some locked_pages -> PageKeySet.add page locked_pages
    | None -> failwith "internal error - extend_tran_locks"
  in
  Hashtbl.replace lm.tran_locks tid locked_pages
;;

let try_acquire_lock lm page tid perm =
  let locked_pages = get_locked_pages lm tid in
  match Hashtbl.find_opt lm.page_locks page, perm with
  | None, _ ->
    Hashtbl.add lm.page_locks page (page_lock_of_perm perm);
    extend_tran_locks lm tid page;
    true
  | Some ExclusiveLock, _ -> PageKeySet.mem page locked_pages
  | Some (SharedLock num_trans), ReadPerm ->
    if not (PageKeySet.mem page locked_pages)
    then (
      Hashtbl.replace lm.page_locks page (SharedLock (num_trans + 1));
      extend_tran_locks lm tid page);
    true
  | Some (SharedLock num_trans), WritePerm
    when num_trans = 1 && PageKeySet.mem page locked_pages ->
    Hashtbl.replace lm.page_locks page ExclusiveLock;
    true
  | Some (SharedLock num_trans), WritePerm -> false
;;

let get_blocked_trans lm page =
  match Hashtbl.find_opt lm.blocked_trans page with
  | Some blocked -> blocked
  | None -> TransactionIdSet.empty
;;

let check_deadlock lm page tid =
  let g = TransactionIdGraph.create () in
  Hashtbl.iter
    (fun t1 locked_pages ->
       PageKeySet.iter
         (fun locked_page ->
            TransactionIdSet.iter
              (fun t2 ->
                 if t1 <> t2
                 then
                   TransactionIdGraph.add_edge
                     g
                     (TransactionIdGraph.V.create t2)
                     (TransactionIdGraph.V.create t1))
              (get_blocked_trans lm page))
         locked_pages)
    lm.tran_locks;
  let num_comps, _ = TransactionIdGraphSCC.scc g in
  let deadlock_free = num_comps = TransactionIdGraph.nb_vertex g in
  deadlock_free
;;

let block_tran lm page tid =
  let blocked_trans = get_blocked_trans lm page in
  Hashtbl.replace lm.blocked_trans page (TransactionIdSet.add tid blocked_trans);
  if check_deadlock lm page tid
  then true
  else (
    Hashtbl.replace lm.blocked_trans page blocked_trans;
    false)
;;

let unblock_tran lm page tid =
  match Hashtbl.find_opt lm.blocked_trans page with
  | Some blocked ->
    Hashtbl.replace lm.blocked_trans page (TransactionIdSet.remove tid blocked)
  | None -> ()
;;

let rec acquire_lock lm page tid perm abort_tran =
  Mutex.lock lm.lock_mutex;
  if try_acquire_lock lm page tid perm
  then (
    Log.log_tid_page tid page "acquired lock with perm %s" (show_permission perm);
    unblock_tran lm page tid;
    Mutex.unlock lm.lock_mutex)
  else (
    if not (block_tran lm page tid)
    then (
      Log.log_tid_page tid page "deadlock detected, aborting transaction";
      abort_tran ();
      Mutex.unlock lm.lock_mutex;
      raise Error.deadlock_victim);
    Log.log_tid_page tid page "blocked on lock with perm %s" (show_permission perm);
    Mutex.unlock lm.lock_mutex;
    Unix.sleepf 0.05;
    acquire_lock lm page tid perm abort_tran)
;;

let release_locks lm tid =
  match Hashtbl.find_opt lm.tran_locks tid with
  | Some locked_pages ->
    PageKeySet.iter
      (fun page ->
         match Hashtbl.find_opt lm.page_locks page with
         | Some ExclusiveLock | Some (SharedLock 1) -> Hashtbl.remove lm.page_locks page
         | Some (SharedLock num_trans) ->
           Hashtbl.replace lm.page_locks page (SharedLock (num_trans - 1))
         | None -> failwith "internal error - release_locks")
      locked_pages;
    Hashtbl.remove lm.tran_locks tid
  | None -> ()
;;

let protect lm f = Mutex.protect lm.lock_mutex f
