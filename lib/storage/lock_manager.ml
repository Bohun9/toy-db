module C = Core
module PageKeySet = Set.Make (Page_key)
module TransactionIdSet = Set.Make (C.Transaction_id)
module TransactionIdGraph = Graph.Imperative.Digraph.Concrete (C.Transaction_id)
module TransactionIdGraphSCC = Graph.Components.Make (TransactionIdGraph)

type lock =
  | Shared of int
  | Exclusive

let lock_of_perm = function
  | Perm.Read -> Shared 1
  | Perm.Write -> Exclusive
;;

type t =
  { mutex : Mutex.t
  ; page_locks : (Page_key.t, lock) Hashtbl.t
  ; tran_locks : (C.Transaction_id.t, PageKeySet.t) Hashtbl.t
  ; blocked_trans : (Page_key.t, TransactionIdSet.t) Hashtbl.t
  }

let create () =
  { mutex = Mutex.create ()
  ; page_locks = Hashtbl.create 16
  ; tran_locks = Hashtbl.create 16
  ; blocked_trans = Hashtbl.create 16
  }
;;

let locked_pages lm tid = Hashtbl.find lm.tran_locks tid
let locked_pages_list lm tid = locked_pages lm tid |> PageKeySet.to_list

let begin_transaction lm tid =
  Mutex.protect lm.mutex (fun () -> Hashtbl.add lm.tran_locks tid PageKeySet.empty)
;;

let add_locked_page lm tid page =
  let new_locked_pages = PageKeySet.add page (locked_pages lm tid) in
  Hashtbl.replace lm.tran_locks tid new_locked_pages
;;

let try_acquire lm page tid perm =
  let locked_pages = locked_pages lm tid in
  match Hashtbl.find_opt lm.page_locks page, perm with
  | None, _ ->
    Hashtbl.add lm.page_locks page (lock_of_perm perm);
    add_locked_page lm tid page;
    true
  | Some Exclusive, _ -> PageKeySet.mem page locked_pages
  | Some (Shared num_trans), Perm.Read ->
    if not (PageKeySet.mem page locked_pages)
    then (
      Hashtbl.replace lm.page_locks page (Shared (num_trans + 1));
      add_locked_page lm tid page);
    true
  | Some (Shared num_trans), Perm.Write
    when num_trans = 1 && PageKeySet.mem page locked_pages ->
    Hashtbl.replace lm.page_locks page Exclusive;
    true
  | Some (Shared num_trans), Perm.Write -> false
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

let rec acquire_lock lm page tid perm abort =
  Mutex.lock lm.mutex;
  if try_acquire lm page tid perm
  then (
    C.Log.log_tid_page tid page "acquired lock with perm %s" (Perm.show perm);
    unblock_tran lm page tid;
    Mutex.unlock lm.mutex)
  else (
    if not (block_tran lm page tid)
    then (
      C.Log.log_tid_page tid page "deadlock detected, aborting transaction";
      abort ();
      Mutex.unlock lm.mutex;
      raise C.Error.deadlock_victim);
    C.Log.log_tid_page tid page "blocked on lock with perm %s" (Perm.show perm);
    Mutex.unlock lm.mutex;
    Unix.sleepf 0.05;
    acquire_lock lm page tid perm abort)
;;

let release_locks lm tid =
  let release_page_lock page =
    match Hashtbl.find lm.page_locks page with
    | Exclusive | Shared 1 -> Hashtbl.remove lm.page_locks page
    | Shared num_trans -> Hashtbl.replace lm.page_locks page (Shared (num_trans - 1))
  in
  PageKeySet.iter release_page_lock (locked_pages lm tid);
  Hashtbl.remove lm.tran_locks tid
;;

let protect lm f = Mutex.protect lm.mutex f
