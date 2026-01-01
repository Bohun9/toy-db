type t =
  { file : string
  ; page_no : int
  }
[@@deriving show { with_path = false }]

let compare = compare
