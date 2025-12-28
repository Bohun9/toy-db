type t =
  | PageKey of
      { file : string
      ; page_no : int
      }
[@@deriving show { with_path = false }]

let compare = compare
