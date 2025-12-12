type page_info =
  { page : Db_page.db_page
  ; flush_page : Db_page.db_page -> unit
  }

type t =
  { cache : (Db_page.page_key, page_info) Hashtbl.t
  ; max_pages : int
  }

let create max_pages = { cache = Hashtbl.create max_pages; max_pages }

let flush_all_pages bp =
  Hashtbl.iter
    (fun _ { page; flush_page } -> if Db_page.is_dirty page then flush_page page)
    bp.cache
;;

let evict bp =
  let k = Hashtbl.fold (fun k _ _ -> Some k) bp.cache None in
  match k with
  | Some k -> Hashtbl.remove bp.cache k
  | None -> failwith "internal error"
;;

let make_space bp = if Hashtbl.length bp.cache == bp.max_pages then evict bp else ()

let get_page bp k load_page flush_page =
  match Hashtbl.find_opt bp.cache k with
  | Some pi -> pi.page
  | None ->
    make_space bp;
    let page = load_page () in
    Hashtbl.add bp.cache k { page; flush_page };
    page
;;
