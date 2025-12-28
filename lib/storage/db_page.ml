type t =
  | DB_HeapPage of Heap_page.t
  | DB_BTreePage of Btree_page.t

let is_dirty = function
  | DB_HeapPage p -> Heap_page.is_dirty p
  | DB_BTreePage p -> Btree_page.is_dirty p
;;
