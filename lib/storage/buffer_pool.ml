open Core

type page_info =
  { page : Db_page.t
  ; flush_page : Db_page.t -> unit
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

let flush_commited_changes bp tid =
  List.iter
    (fun page_key ->
       match Hashtbl.find_opt bp.cache page_key with
       | Some pinfo when Db_page.is_dirty pinfo.page -> pinfo.flush_page pinfo.page
       | _ -> ())
    (Lock_manager.get_locked_pages_list bp.lock_manager tid)
;;

let discard_aborted_changes bp tid =
  List.iter
    (fun page_key ->
       match Hashtbl.find_opt bp.cache page_key with
       | Some pinfo when Db_page.is_dirty pinfo.page -> Hashtbl.remove bp.cache page_key
       | _ -> ())
    (Lock_manager.get_locked_pages_list bp.lock_manager tid)
;;

let begin_transaction bp tid = Lock_manager.begin_transaction bp.lock_manager tid

let commit_transaction bp tid =
  Lock_manager.protect bp.lock_manager (fun () ->
    Mutex.protect bp.cache_mutex (fun () -> flush_commited_changes bp tid);
    Lock_manager.release_locks bp.lock_manager tid)
;;

let abort_transaction_locked bp tid () =
  Mutex.protect bp.cache_mutex (fun () -> discard_aborted_changes bp tid);
  Lock_manager.release_locks bp.lock_manager tid
;;

let abort_transaction bp tid =
  Lock_manager.protect bp.lock_manager (abort_transaction_locked bp tid)
;;

let acquire_lock bp page tid perm =
  Lock_manager.acquire_lock
    bp.lock_manager
    page
    tid
    perm
    (abort_transaction_locked bp tid)
;;

let flush_all_pages bp =
  Hashtbl.iter
    (fun _ { page; flush_page } -> if Db_page.is_dirty page then flush_page page)
    bp.cache
;;

let evict bp =
  match
    Hashtbl.to_seq bp.cache
    |> Seq.find (fun (_, pinfo) -> not (Db_page.is_dirty pinfo.page))
  with
  | Some (pk, _) -> Hashtbl.remove bp.cache pk
  | None -> raise (Error.DBError Error.Buffer_pool_overflow)
;;

let make_space bp = if Hashtbl.length bp.cache = bp.max_pages then evict bp else ()

let get_page bp pk tid perm load_page flush_page =
  acquire_lock bp pk tid perm;
  Mutex.protect bp.cache_mutex (fun () ->
    match Hashtbl.find_opt bp.cache pk with
    | Some pinfo -> pinfo.page
    | None ->
      make_space bp;
      let page = load_page () in
      Hashtbl.add bp.cache pk { page; flush_page };
      page)
;;
