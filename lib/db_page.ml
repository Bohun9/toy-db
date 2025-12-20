type t = DB_HeapPage of Heap_page.t

let is_dirty = function
  | DB_HeapPage hf -> Heap_page.is_dirty hf
;;
