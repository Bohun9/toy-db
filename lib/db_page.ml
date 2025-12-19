type t = DB_HeapPage of Heap_page.heap_page

let is_dirty = function
  | DB_HeapPage hf -> Heap_page.is_dirty hf
;;
