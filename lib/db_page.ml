type db_page = DB_HeapPage of Heap_page.heap_page

type page_key =
  | PageKey of
      { file : string
      ; page_no : int
      }

module PageKeySet = Set.Make (struct
    type t = page_key

    let compare = compare
  end)

let is_dirty = function
  | DB_HeapPage hf -> Heap_page.is_dirty hf
;;
