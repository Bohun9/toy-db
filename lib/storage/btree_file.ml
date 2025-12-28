open Core
open Metadata

type t =
  { file : string
  ; schema : Table_schema.t
  ; buf_pool : Buffer_pool.t
  ; num_pages : int Atomic.t
  ; key_field : int
  }

let file_path f = f.file
let page_key f page_no = Page_key.PageKey { file = f.file; page_no }
let get_key f t = Tuple.field t f.key_field
let fresh_page_no f = Atomic.fetch_and_add f.num_pages 1
let num_pages f = Atomic.get f.num_pages
let schema f = f.schema

let flush_btree_page f p =
  p |> Btree_page.serialize |> Storage_layout.flush_raw_page f.file (Btree_page.page_no p);
  Btree_page.clear_dirty p
;;

let flush_header_page f p = flush_btree_page f (Btree_page.HeaderPage p)

let flush_internal_page f p =
  flush_btree_page f (Btree_page.NodePage (Btree_page.InternalPage p))
;;

let flush_leaf_page f p = flush_btree_page f (Btree_page.NodePage (Btree_page.LeafPage p))

let flush_page f = function
  | Db_page.DB_BTreePage p -> flush_btree_page f p
  | _ -> failwith "internal error"
;;

let initialize f key_field =
  let header_page_no = fresh_page_no f in
  let root_page_no = fresh_page_no f in
  assert (header_page_no = 0);
  assert (root_page_no = 1);
  let header = Btree_header_page.create root_page_no in
  let root = Btree_leaf_page.create root_page_no key_field None [] None in
  flush_header_page f header;
  flush_leaf_page f root
;;

let create file schema buf_pool key_field =
  let num_pages = Storage_layout.get_num_pages file in
  let f = { file; schema; buf_pool; num_pages = Atomic.make num_pages; key_field } in
  if num_pages = 0 then initialize f key_field;
  f
;;

let load_header_page f =
  Storage_layout.load_raw_page f.file 0
  |> Btree_header_page.deserialize
  |> fun p -> Btree_page.HeaderPage p |> fun p -> Db_page.DB_BTreePage p
;;

let load_node_page f page_no =
  Storage_layout.load_raw_page f.file page_no
  |> Btree_page.deserialize page_no f.schema f.key_field
  |> fun p -> Btree_page.NodePage p |> fun p -> Db_page.DB_BTreePage p
;;

let get_page f page_no tid perm load_page flush_page =
  let db_page =
    Buffer_pool.get_page f.buf_pool (page_key f page_no) tid perm load_page flush_page
  in
  match db_page with
  | Db_page.DB_BTreePage p -> p
  | _ -> failwith "internal error"
;;

let get_header_page f tid perm =
  let p = get_page f 0 tid perm (fun () -> load_header_page f) (flush_page f) in
  match p with
  | Btree_page.HeaderPage p -> p
  | _ -> failwith "internal error"
;;

let get_node_page f page_no tid perm =
  let p =
    get_page f page_no tid perm (fun () -> load_node_page f page_no) (flush_page f)
  in
  match p with
  | Btree_page.NodePage p -> p
  | _ -> failwith "internal error"
;;

let get_node_page_read f page_no tid = get_node_page f page_no tid Lock_manager.ReadPerm

let get_leaf_page f page_no tid perm =
  let p = get_node_page f page_no tid perm in
  match p with
  | Btree_page.LeafPage p -> p
  | _ -> failwith ""
;;

let get_internal_page f page_no tid perm =
  let p = get_node_page f page_no tid perm in
  match p with
  | Btree_page.InternalPage p -> p
  | _ -> failwith ""
;;

let get_root f tid =
  let p = get_header_page f tid Lock_manager.ReadPerm in
  Btree_header_page.root p
;;

let set_root f new_root tid =
  let p = get_header_page f tid Lock_manager.WritePerm in
  Btree_header_page.set_root p new_root
;;

let get_parent f n tid perm =
  let parent_page_no = Btree_page.parent n in
  get_internal_page f parent_page_no tid perm
;;

let get_parent_write f n tid = get_parent f n tid Lock_manager.WritePerm
let get_leaf_parent f leaf = get_parent f (LeafPage leaf)

let find_leaf f k tid perm =
  let root = get_root f tid in
  let rec go n =
    match get_node_page_read f n tid with
    | Btree_page.LeafPage _ -> get_leaf_page f n tid perm
    | Btree_page.InternalPage p -> go (Btree_internal_page.find_child p k)
  in
  go root
;;

let create_leaf_node f parent tuples next_leaf_page tid =
  let page_no = fresh_page_no f in
  let new_leaf =
    Btree_leaf_page.create page_no f.key_field parent tuples next_leaf_page
  in
  flush_leaf_page f new_leaf;
  get_leaf_page f page_no tid Lock_manager.WritePerm
;;

let create_internal_node f parent keys children tid =
  let page_no = fresh_page_no f in
  let new_internal = Btree_internal_page.create page_no parent keys children in
  flush_internal_page f new_internal;
  get_internal_page f page_no tid Lock_manager.WritePerm
;;

let node_page_no n = Btree_page.page_no (Btree_page.NodePage n)
let is_root f n tid = node_page_no n = get_root f tid
let is_leaf_root f leaf = is_root f (Btree_page.LeafPage leaf)
let is_internal_root f internal = is_root f (Btree_page.InternalPage internal)

let update_parent_pointers f children father tid =
  List.iter
    (fun child_page_no ->
       let child = get_node_page f child_page_no tid Lock_manager.WritePerm in
       Btree_page.set_parent child father)
    children
;;

(* [n'] was created from splitting [n].
   Insert ([k], [n']) into the parent of [n]. *)
let rec insert_in_parent f n k n' tid =
  if is_root f n tid
  then (
    let children = [ node_page_no n; n' ] in
    let new_root = create_internal_node f None [ k ] children tid in
    let new_root_page_no = Btree_node.page_no new_root in
    update_parent_pointers f children new_root_page_no tid;
    set_root f new_root_page_no tid)
  else (
    let p = get_parent_write f n tid in
    match Btree_internal_page.insert_entry p k n' with
    | Btree_internal_page.Inserted -> ()
    | Btree_internal_page.Split { sep_key; keys; children } ->
      Log.log "btree internal split";
      let p' = create_internal_node f (Btree_node.parent_opt p) keys children tid in
      update_parent_pointers f children (Btree_node.page_no p') tid;
      insert_in_parent f (Btree_page.InternalPage p) sep_key (Btree_node.page_no p') tid)
;;

let insert_tuple f t tid =
  let k = get_key f t in
  let leaf = find_leaf f k tid Lock_manager.WritePerm in
  match Btree_leaf_page.insert_tuple leaf t with
  | Btree_leaf_page.Inserted -> ()
  | Btree_leaf_page.Split new_leaf_tuples ->
    Log.log "btree leaf split";
    let new_leaf =
      create_leaf_node
        f
        (Btree_node.parent_opt leaf)
        new_leaf_tuples
        (Btree_leaf_page.next_leaf_page leaf)
        tid
    in
    Btree_leaf_page.set_next_leaf_page leaf (Some (Btree_node.page_no new_leaf));
    insert_in_parent
      f
      (Btree_page.LeafPage leaf)
      (Btree_leaf_page.lowest_key new_leaf)
      (Btree_node.page_no new_leaf)
      tid
;;

let get_sibling f n tid =
  let parent = get_parent f n tid Lock_manager.WritePerm in
  Btree_internal_page.get_sibling parent (node_page_no n), parent
;;

type leaf_sibling =
  { leaf : Btree_leaf_page.t
  ; sep_key : Value.t
  ; left : bool
  }

let get_leaf_sibling f leaf tid =
  let sibling, parent = get_sibling f (LeafPage leaf) tid in
  ( { leaf = get_leaf_page f sibling.node tid Lock_manager.WritePerm
    ; sep_key = sibling.sep_key
    ; left = sibling.left
    }
  , parent )
;;

type internal_sibling =
  { internal : Btree_internal_page.t
  ; sep_key : Value.t
  ; left : bool
  }

let get_internal_sibling f internal tid =
  let sibling, parent = get_sibling f (InternalPage internal) tid in
  ( { internal = get_internal_page f sibling.node tid Lock_manager.WritePerm
    ; sep_key = sibling.sep_key
    ; left = sibling.left
    }
  , parent )
;;

let rec delete_entry f internal k child tid =
  match
    Btree_internal_page.delete_entry internal k child (is_internal_root f internal tid)
  with
  | Btree_internal_page.Deleted -> ()
  | Btree_internal_page.EmptyRoot { new_root } ->
    Log.log "btree empty root";
    set_root f new_root tid
  | Btree_internal_page.Underfull ->
    Log.log "internal underfull";
    let sibling, parent = get_internal_sibling f internal tid in
    if Btree_internal_page.can_coalesce internal sibling.internal
    then (
      Log.log "internal coalescing";
      let left, right =
        if sibling.left then sibling.internal, internal else internal, sibling.internal
      in
      let moved_children, new_father =
        Btree_internal_page.coalesce left sibling.sep_key right
      in
      update_parent_pointers f moved_children new_father tid;
      delete_entry f parent sibling.sep_key (Btree_node.page_no right) tid)
    else if sibling.left
    then (
      Log.log "internal left redistribution";
      let borrowed_key, borrowed_child =
        Btree_internal_page.delete_highest_entry sibling.internal
      in
      Btree_internal_page.insert_entry_at_beggining
        internal
        borrowed_child
        sibling.sep_key;
      Btree_internal_page.replace_key parent sibling.sep_key borrowed_key;
      update_parent_pointers f [ borrowed_child ] (Btree_node.page_no internal) tid)
    else (
      Log.log "internal right redistribution";
      let borrowed_child, borrowed_key =
        Btree_internal_page.delete_lowest_entry sibling.internal
      in
      Btree_internal_page.insert_entry_at_end internal sibling.sep_key borrowed_child;
      Btree_internal_page.replace_key parent sibling.sep_key borrowed_key;
      update_parent_pointers f [ borrowed_child ] (Btree_node.page_no internal) tid)
;;

let delete_tuple f t tid =
  let k = get_key f t in
  let leaf = find_leaf f k tid Lock_manager.WritePerm in
  match Btree_leaf_page.delete_tuple leaf t (is_leaf_root f leaf tid) with
  | Btree_leaf_page.Deleted -> ()
  | Btree_leaf_page.Underfull ->
    Log.log "leaf underfull";
    let sibling, parent = get_leaf_sibling f leaf tid in
    if Btree_leaf_page.can_coalesce leaf sibling.leaf
    then (
      Log.log "leaf coalescing";
      let left, right = if sibling.left then sibling.leaf, leaf else leaf, sibling.leaf in
      Btree_leaf_page.coalesce left right;
      delete_entry f parent sibling.sep_key (Btree_node.page_no right) tid)
    else if sibling.left
    then (
      Log.log "leaf left redistribution";
      let borrowed_tuple = Btree_leaf_page.delete_highest_tuple sibling.leaf in
      Btree_leaf_page.insert_tuple_no_split leaf borrowed_tuple;
      Btree_internal_page.replace_key parent sibling.sep_key (get_key f borrowed_tuple))
    else (
      Log.log "leaf right redistribution";
      let borrowed_tuple = Btree_leaf_page.delete_lowest_tuple sibling.leaf in
      Btree_leaf_page.insert_tuple_no_split leaf borrowed_tuple;
      Btree_internal_page.replace_key
        parent
        sibling.sep_key
        (Btree_leaf_page.lowest_key sibling.leaf))
;;

let range_scan f interval tid =
  Log.log "hello from range_scan";
  let leaf =
    find_leaf f (Value_interval.left_endpoint interval) tid Lock_manager.ReadPerm
  in
  let rec scan_from_leaf leaf =
    Seq.append
      (Seq.filter
         (fun t -> Value_interval.inside interval (get_key f t))
         (Btree_leaf_page.scan_page leaf))
      (fun () ->
         match Btree_leaf_page.next_leaf_page leaf with
         | Some next_page_no ->
           let next_leaf = get_leaf_page f next_page_no tid Lock_manager.ReadPerm in
           scan_from_leaf next_leaf ()
         | None -> Seq.Nil)
  in
  Seq.take_while
    (fun t -> Value_interval.satisfies_upper_bound interval (get_key f t))
    (scan_from_leaf leaf)
;;

let key_info f = List.nth (Table_schema.columns f.schema) f.key_field
let scan_file f tid = range_scan f (Value_interval.unbounded (key_info f).typ) tid
