type t =
  { attributes : Value.t list
  ; rid : Record_id.t option
  }
[@@deriving show]

type tuples = t list [@@deriving show]

let trans_tuple t = { attributes = List.map Value.trans t; rid = None }
let combine_tuple t1 t2 = { attributes = t1.attributes @ t2.attributes; rid = None }
let attribute t n = List.nth t.attributes n
