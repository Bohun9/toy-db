module C = Core
module M = Metadata
module Header = Page.Btree_header_page
module Internal = Page.Btree_internal_page
module Leaf = Page.Btree_leaf_page
module Node = Page.Btree_node
module Btree_op = Page.Btree_op
module Db_page = Page.Db_page
module Generic_page = Page.Generic_page
module Page = Page.Btree_page

let log = C.Log.log

type t =
  { file : string
  ; schema : M.Table_schema.t
  ; buf_pool : Buffer_pool.t
  ; num_pages : int Atomic.t
  ; key_attribute : int
  }

let file_path f = f.file
let schema f = f.schema
let fresh_page_no f = Atomic.fetch_and_add f.num_pages 1
let page_key f page_no = { Page_key.file = f.file; page_no }
let key_info f = List.nth (M.Table_schema.columns f.schema) f.key_attribute

let flush_btree_page f p =
  p |> Page.serialize |> Page_io.write_page f.file (Page.page_no p);
  Page.clear_dirty p
;;

let flush_header_page f p = flush_btree_page f (Page.HeaderPage p)
let flush_internal_page f p = flush_btree_page f (Page.NodePage (Node.InternalPage p))
let flush_leaf_page f p = flush_btree_page f (Page.NodePage (Node.LeafPage p))

let flush_page f = function
  | Db_page.DB_BTreePage p -> flush_btree_page f p
  | _ -> failwith "internal error"
;;

let initialize f key_attribute =
  let header_page_no = fresh_page_no f in
  let root_page_no = fresh_page_no f in
  assert (header_page_no = 0);
  assert (root_page_no = 1);
  let header = Header.create header_page_no root_page_no in
  let root = Leaf.create root_page_no f.schema key_attribute None None in
  flush_header_page f header;
  flush_leaf_page f root
;;

let create file schema buf_pool key_attribute =
  let num_pages = Page_io.num_pages file in
  let f = { file; schema; buf_pool; num_pages = Atomic.make num_pages; key_attribute } in
  if num_pages = 0 then initialize f key_attribute;
  f
;;

let load_header_page f =
  Page_io.read_page f.file 0
  |> Header.deserialize 0
  |> fun p -> Db_page.DB_BTreePage (Page.HeaderPage p)
;;

let load_node_page f page_no =
  Page_io.read_page f.file page_no
  |> Node.deserialize page_no f.schema f.key_attribute
  |> fun p -> Db_page.DB_BTreePage (Page.NodePage p)
;;

module Make (Ctx : sig
    val f : t
    val tid : C.Transaction_id.t
  end) =
struct
  let key_of_tuple t = C.Tuple.attribute t Ctx.f.key_attribute

  let get_page page_no perm load_page flush_page =
    let db_page =
      Buffer_pool.get_page
        Ctx.f.buf_pool
        (page_key Ctx.f page_no)
        Ctx.tid
        perm
        load_page
        flush_page
    in
    match db_page with
    | Db_page.DB_BTreePage p -> p
    | _ -> failwith "internal error"
  ;;

  let get_header_page perm =
    let p = get_page 0 perm (fun () -> load_header_page Ctx.f) (flush_page Ctx.f) in
    match p with
    | Page.HeaderPage p -> p
    | _ -> failwith "internal error"
  ;;

  let get_node_page page_no perm =
    let p =
      get_page page_no perm (fun () -> load_node_page Ctx.f page_no) (flush_page Ctx.f)
    in
    match p with
    | Page.NodePage p -> p
    | _ -> failwith "internal error"
  ;;

  let get_leaf_page page_no perm =
    let p = get_node_page page_no perm in
    match p with
    | Node.LeafPage p -> p
    | _ -> failwith ""
  ;;

  let get_internal_page page_no perm =
    let p = get_node_page page_no perm in
    match p with
    | Node.InternalPage p -> p
    | _ -> failwith ""
  ;;

  let get_root () =
    let p = get_header_page Perm.Read in
    Header.root p
  ;;

  let set_root new_root =
    let p = get_header_page Perm.Write in
    Header.set_root p new_root
  ;;

  let release_path_locks path =
    List.iter
      (fun p ->
         Buffer_pool.unsafe_release_lock
           Ctx.f.buf_pool
           (page_key Ctx.f (Generic_page.page_no p))
           Ctx.tid)
      path
  ;;

  let find_leaf k op =
    let perm =
      match op with
      | Btree_op.Read -> Perm.Read
      | Btree_op.Insert -> Perm.Write
      | Btree_op.Delete -> Perm.Write
    in
    let root = get_root () in
    let rec search n path =
      let node = get_node_page n perm in
      let path =
        if Node.safe node op
        then (
          release_path_locks path;
          [])
        else path
      in
      match node with
      | Node.LeafPage _ -> get_leaf_page n perm, path
      | Node.InternalPage p -> search (Internal.find_child p k) (p :: path)
    in
    search root []
  ;;

  let create_leaf_node tuples_info next_leaf_page =
    let page_no = fresh_page_no Ctx.f in
    let new_leaf =
      Leaf.create
        page_no
        Ctx.f.schema
        Ctx.f.key_attribute
        (Some tuples_info)
        next_leaf_page
    in
    flush_leaf_page Ctx.f new_leaf;
    get_leaf_page page_no Perm.Write
  ;;

  let node_page_no n = Page.page_no (Page.NodePage n)
  let is_root n = node_page_no n = get_root ()
  let is_leaf_root leaf = is_root (Node.LeafPage leaf)
  let is_internal_root internal = is_root (Node.InternalPage internal)

  let create_internal_node create_data =
    let page_no = fresh_page_no Ctx.f in
    let new_internal = Internal.create page_no (key_info Ctx.f).typ create_data in
    flush_internal_page Ctx.f new_internal;
    get_internal_page page_no Perm.Write
  ;;

  let rec insert_in_parent n1 k n2 path =
    if is_root n1
    then (
      let new_root =
        create_internal_node
          (Internal.RootData { child1 = node_page_no n1; key = k; child2 = n2 })
      in
      set_root (Generic_page.page_no new_root))
    else (
      let p1, path = List.hd path, List.tl path in
      match Internal.insert_entry p1 k n2 with
      | Internal.Inserted -> ()
      | Internal.Split { sep_key; internal_data } ->
        log "btree internal split";
        let p2 = create_internal_node (InternalData internal_data) in
        insert_in_parent (Node.InternalPage p1) sep_key (Generic_page.page_no p2) path)
  ;;

  let insert_tuple t =
    let k = key_of_tuple t in
    let leaf, path = find_leaf k Btree_op.Insert in
    match Leaf.insert_tuple leaf t with
    | Leaf.Inserted -> ()
    | Leaf.Split tuples_info ->
      let new_leaf = create_leaf_node tuples_info (Leaf.next_leaf leaf) in
      Leaf.set_next_leaf leaf (Some (Generic_page.page_no new_leaf));
      insert_in_parent
        (Node.LeafPage leaf)
        (Leaf.lowest_key new_leaf)
        (Generic_page.page_no new_leaf)
        path
  ;;

  let get_sibling parent n = Internal.get_sibling parent (node_page_no n)

  type leaf_sibling =
    { leaf : Leaf.t
    ; sep_key : C.Value.t
    ; left : bool
    }

  let get_leaf_sibling leaf parent =
    let sibling = get_sibling parent (LeafPage leaf) in
    { leaf = get_leaf_page sibling.node Perm.Write
    ; sep_key = sibling.sep_key
    ; left = sibling.left
    }
  ;;

  type internal_sibling =
    { internal : Internal.t
    ; sep_key : C.Value.t
    ; left : bool
    }

  let get_internal_sibling internal parent =
    let sibling = get_sibling parent (InternalPage internal) in
    { internal = get_internal_page sibling.node Perm.Write
    ; sep_key = sibling.sep_key
    ; left = sibling.left
    }
  ;;

  let rec delete_entry internal k child path =
    match Internal.delete_entry internal k child (is_internal_root internal) with
    | Internal.Deleted -> ()
    | Internal.EmptyRoot { new_root } -> set_root new_root
    | Internal.Underfull ->
      let parent, path = List.hd path, List.tl path in
      let sibling = get_internal_sibling internal parent in
      if Internal.can_coalesce internal sibling.internal
      then (
        let left, right =
          if sibling.left then sibling.internal, internal else internal, sibling.internal
        in
        Internal.coalesce left sibling.sep_key right;
        delete_entry parent sibling.sep_key (Generic_page.page_no right) path)
      else (
        let borrowed_key =
          if sibling.left
          then (
            let borrowed_key, borrowed_child =
              Internal.delete_highest_entry sibling.internal
            in
            Internal.insert_entry_at_beginning internal borrowed_child sibling.sep_key;
            borrowed_key)
          else (
            let borrowed_child, borrowed_key =
              Internal.delete_lowest_entry sibling.internal
            in
            Internal.insert_entry_at_end internal sibling.sep_key borrowed_child;
            borrowed_key)
        in
        Internal.replace_key parent sibling.sep_key borrowed_key)
  ;;

  let delete_tuple t =
    let k = key_of_tuple t in
    let leaf, path = find_leaf k Btree_op.Delete in
    match Leaf.delete_tuple leaf t (is_leaf_root leaf) with
    | Leaf.Deleted -> ()
    | Leaf.Underfull ->
      let parent, path = List.hd path, List.tl path in
      let sibling = get_leaf_sibling leaf parent in
      if Leaf.can_coalesce leaf sibling.leaf
      then (
        let left, right =
          if sibling.left then sibling.leaf, leaf else leaf, sibling.leaf
        in
        Leaf.coalesce left right;
        delete_entry parent sibling.sep_key (Generic_page.page_no right) path)
      else (
        let borrowed_tuple, subst_key =
          if sibling.left
          then (
            let borrowed_tuple = Leaf.delete_highest_tuple sibling.leaf in
            borrowed_tuple, key_of_tuple borrowed_tuple)
          else (
            let borrowed_tuple = Leaf.delete_lowest_tuple sibling.leaf in
            borrowed_tuple, Leaf.lowest_key sibling.leaf)
        in
        Leaf.insert_tuple_no_split leaf borrowed_tuple;
        Internal.replace_key parent sibling.sep_key subst_key)
  ;;

  let range_scan interval =
    let leaf, _ = find_leaf (C.Value_interval.left_endpoint interval) Btree_op.Read in
    let rec scan_from_leaf leaf =
      Seq.append
        (Seq.filter
           (fun t -> C.Value_interval.satisfies_lower_bound interval (key_of_tuple t))
           (Leaf.scan_page leaf))
        (fun () ->
           match Leaf.next_leaf leaf with
           | Some next_page_no ->
             let next_leaf = get_leaf_page next_page_no Perm.Read in
             scan_from_leaf next_leaf ()
           | None -> Seq.Nil)
    in
    Seq.take_while
      (fun t -> C.Value_interval.satisfies_upper_bound interval (key_of_tuple t))
      (scan_from_leaf leaf)
  ;;

  let scan_file () = range_scan (C.Value_interval.unbounded (key_info Ctx.f).typ)
end

type btree_ops =
  { insert_tuple : C.Tuple.t -> unit
  ; delete_tuple : C.Tuple.t -> unit
  ; range_scan : C.Value_interval.t -> C.Tuple.t Seq.t
  ; scan_file : unit -> C.Tuple.t Seq.t
  }

let make_ops f tid =
  let module B =
    Make (struct
      let f = f
      let tid = tid
    end)
  in
  { insert_tuple = B.insert_tuple
  ; delete_tuple = B.delete_tuple
  ; range_scan = B.range_scan
  ; scan_file = B.scan_file
  }
;;

let insert_tuple f t tid = (make_ops f tid).insert_tuple t
let delete_tuple f t tid = (make_ops f tid).delete_tuple t
let range_scan f interval tid = (make_ops f tid).range_scan interval
let scan_file f tid = (make_ops f tid).scan_file ()
