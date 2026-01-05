module C = Core

type page_info =
  { page : Db_page.t
  ; flush : Db_page.t -> unit
  }

type t =
  { cache_mutex : Mutex.t
  ; cache : (Page_key.t, page_info) Hashtbl.t
  ; max_pages : int
  ; lock_manager : Lock_manager.t
  }

let create max_pages lock_manager =
  { cache_mutex = Mutex.create ()
  ; cache = Hashtbl.create max_pages
  ; max_pages
  ; lock_manager
  }
;;

let flush_committed_changes bp tid =
  List.iter
    (fun page_key ->
       match Hashtbl.find_opt bp.cache page_key with
       | Some { page; flush } when Db_page.is_dirty page -> flush page
       | _ -> ())
    (Lock_manager.locked_pages_list bp.lock_manager tid)
;;

let discard_aborted_changes bp tid =
  List.iter
    (fun page_key ->
       match Hashtbl.find_opt bp.cache page_key with
       | Some { page; _ } when Db_page.is_dirty page -> Hashtbl.remove bp.cache page_key
       | _ -> ())
    (Lock_manager.locked_pages_list bp.lock_manager tid)
;;

let begin_transaction bp tid = Lock_manager.begin_transaction bp.lock_manager tid

let commit_transaction bp tid =
  Lock_manager.protect bp.lock_manager (fun () ->
    Mutex.protect bp.cache_mutex (fun () -> flush_committed_changes bp tid);
    Lock_manager.release_locks bp.lock_manager tid)
;;

let abort_transaction_locked bp tid =
  Mutex.protect bp.cache_mutex (fun () -> discard_aborted_changes bp tid);
  Lock_manager.release_locks bp.lock_manager tid
;;

let abort_transaction bp tid =
  Lock_manager.protect bp.lock_manager (fun () -> abort_transaction_locked bp tid)
;;

let acquire_lock bp page tid perm =
  Lock_manager.acquire_lock bp.lock_manager page tid perm (fun () ->
    abort_transaction_locked bp tid)
;;

let evict bp =
  match
    Hashtbl.to_seq bp.cache
    |> Seq.find (fun (_, pinfo) -> not (Db_page.is_dirty pinfo.page))
  with
  | Some (k, _) -> Hashtbl.remove bp.cache k
  | None -> raise C.Error.buffer_pool_overflow
;;

let make_space bp = if Hashtbl.length bp.cache = bp.max_pages then evict bp

let get_page bp k tid perm load flush =
  acquire_lock bp k tid perm;
  Mutex.protect bp.cache_mutex (fun () ->
    match Hashtbl.find_opt bp.cache k with
    | Some pinfo -> pinfo.page
    | None ->
      make_space bp;
      let page = load () in
      Hashtbl.add bp.cache k { page; flush };
      page)
;;

let discard_file_pages bp file =
  Mutex.protect bp.cache_mutex (fun () ->
    Hashtbl.filter_map_inplace
      (fun (page_key : Page_key.t) p -> if file = page_key.file then None else Some p)
      bp.cache)
;;

let unsafe_release_lock bp k tid =
  Mutex.protect bp.cache_mutex (fun () ->
    match Hashtbl.find_opt bp.cache k with
    | Some pinfo when Db_page.is_dirty pinfo.page -> ()
    | _ -> Lock_manager.unsafe_release_lock bp.lock_manager tid k)
;;
