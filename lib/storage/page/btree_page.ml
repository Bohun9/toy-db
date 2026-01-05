type t =
  | HeaderPage of Btree_header_page.t
  | NodePage of Btree_node.t

module Node = Btree_node

let serialize = function
  | HeaderPage p -> Btree_header_page.serialize p
  | NodePage (Node.InternalPage p) -> Btree_internal_page.serialize p
  | NodePage (Node.LeafPage p) -> Btree_leaf_page.serialize p
;;

let page_no = function
  | HeaderPage p -> Generic_page.page_no p
  | NodePage (Node.InternalPage p) -> Generic_page.page_no p
  | NodePage (Node.LeafPage p) -> Generic_page.page_no p
;;

let dirty = function
  | HeaderPage p -> Generic_page.dirty p
  | NodePage (Node.InternalPage p) -> Generic_page.dirty p
  | NodePage (Node.LeafPage p) -> Generic_page.dirty p
;;

let clear_dirty = function
  | HeaderPage p -> Generic_page.clear_dirty p
  | NodePage (Node.InternalPage p) -> Generic_page.clear_dirty p
  | NodePage (Node.LeafPage p) -> Generic_page.clear_dirty p
;;
