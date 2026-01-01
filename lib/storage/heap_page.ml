type t =
  | HeaderPage of Heap_header_page.t
  | DataPage of Heap_data_page.t

let serialize = function
  | HeaderPage p -> Heap_header_page.serialize p
  | DataPage p -> Heap_data_page.serialize p
;;

let page_no = function
  | HeaderPage p -> Generic_page.page_no p
  | DataPage p -> Generic_page.page_no p
;;

let is_dirty = function
  | HeaderPage p -> Generic_page.is_dirty p
  | DataPage p -> Generic_page.is_dirty p
;;

let set_dirty = function
  | HeaderPage p -> Generic_page.set_dirty p
  | DataPage p -> Generic_page.set_dirty p
;;

let clear_dirty = function
  | HeaderPage p -> Generic_page.clear_dirty p
  | DataPage p -> Generic_page.clear_dirty p
;;
