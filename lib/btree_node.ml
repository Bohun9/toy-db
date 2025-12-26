type 'a t =
  { mutable parent : int option
  ; data : 'a
  }

let create parent data = { parent; data }
let parent_opt n = n.parent
let parent n = Option.get n.parent

let set_parent n v =
  n.parent <- v;
  Generic_page.set_dirty n.data
;;

let encode_parent p = Option.value p ~default:0

let decode_parent = function
  | 0 -> None
  | p -> Some p
;;

let page_no n = Generic_page.page_no n.data
let is_dirty n = Generic_page.is_dirty n.data
let set_dirty n = Generic_page.set_dirty n.data
let clear_dirty n = Generic_page.clear_dirty n.data
