type t =
  | Int
  | String
[@@deriving show]

let show = function
  | Int -> "INT"
  | String -> "STRING"
;;
