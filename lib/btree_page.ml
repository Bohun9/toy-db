type node_page =
  | InternalPage of Btree_internal_page.t
  | LeafPage of Btree_leaf_page.t

type t =
  | HeaderPage of Btree_header_page.t
  | NodePage of node_page

let serialize = function
  | HeaderPage p -> Btree_header_page.serialize p
  | NodePage (InternalPage p) -> Btree_internal_page.serialize p
  | NodePage (LeafPage p) -> Btree_leaf_page.serialize p
;;

let deserialize page_no schema key_field data =
  match Bytes.get data 0 with
  | c when c = Btree_internal_page.node_type_id ->
    Btree_internal_page.deserialize
      page_no
      (Table_schema.column_type schema key_field)
      data
    |> fun p -> InternalPage p
  | c when c = Btree_leaf_page.node_type_id ->
    Btree_leaf_page.deserialize page_no schema key_field data |> fun p -> LeafPage p
  | _ -> failwith "internal error"
;;

let page_no = function
  | HeaderPage p -> Generic_page.page_no p
  | NodePage (InternalPage p) -> Btree_node.page_no p
  | NodePage (LeafPage p) -> Btree_node.page_no p
;;

let parent = function
  | InternalPage p -> Btree_node.parent p
  | LeafPage p -> Btree_node.parent p
;;

let set_parent n parent =
  match n with
  | InternalPage p -> Btree_node.set_parent p (Some parent)
  | LeafPage p -> Btree_node.set_parent p (Some parent)
;;

let is_dirty = function
  | HeaderPage p -> Generic_page.is_dirty p
  | NodePage (InternalPage p) -> Btree_node.is_dirty p
  | NodePage (LeafPage p) -> Btree_node.is_dirty p
;;

let clear_dirty = function
  | HeaderPage p -> Generic_page.clear_dirty p
  | NodePage (InternalPage p) -> Btree_node.clear_dirty p
  | NodePage (LeafPage p) -> Btree_node.clear_dirty p
;;
