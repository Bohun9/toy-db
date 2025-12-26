type record_id =
  | RecordID of
      { page_no : int
      ; slot_idx : int
      }
[@@deriving show]

type t =
  { values : Value.t list
  ; rid : record_id option
  }
[@@deriving show]

type tuples = t list [@@deriving show]

let trans_tuple t = { values = List.map Value.trans t; rid = None }
let combine_tuple t1 t2 = { values = t1.values @ t2.values; rid = None }
let field t n = List.nth t.values n
