type t =
  | Read
  | Write
[@@deriving show { with_path = false }]
