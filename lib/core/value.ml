let int_size = 8
let string_max_length = 32

type t =
  | Int of int
  | String of string
[@@deriving show]

let compare = compare
let min = min
let max = max

let eval_relop v1 op v2 =
  match op with
  | Syntax.Eq -> v1 = v2
  | Syntax.Le -> v1 <= v2
  | Syntax.Lt -> v1 < v2
  | Syntax.Ge -> v1 >= v2
  | Syntax.Gt -> v1 > v2
;;

let eval_le v1 v2 = eval_relop v1 Syntax.Le v2
let eval_lt v1 v2 = eval_relop v1 Syntax.Lt v2
let eval_ge v1 v2 = eval_relop v1 Syntax.Ge v2
let eval_gt v1 v2 = eval_relop v1 Syntax.Gt v2

let value_to_int = function
  | Int n -> n
  | _ -> failwith "internal error - value_to_int"
;;

let value_to_string = function
  | String s -> s
  | _ -> failwith "internal error -  value_to_string"
;;

let derive_type = function
  | Int _ -> Type.Int
  | String _ -> Type.String
;;

let trans = function
  | Syntax.VInt n -> Int n
  | Syntax.VString s -> String s
;;

let minus_infty = function
  | Type.Int -> Int Int.min_int
  | Type.String -> String ""
;;

let plus_infty = function
  | Type.Int -> Int Int.max_int
  | Type.String -> String (String.make string_max_length '\xFF')
;;
