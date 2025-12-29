let int_size = 8
let string_max_length = 32

type t =
  | VInt of int
  | VString of string
[@@deriving show]

let compare = compare

let derive_type = function
  | VInt _ -> Type.TInt
  | VString _ -> Type.TString
;;

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
  | VInt n -> n
  | _ -> failwith "internal error - value_to_int"
;;

let value_to_string = function
  | VString s -> s
  | _ -> failwith "internal error -  value_to_string"
;;

let derive_type = function
  | VInt _ -> Type.TInt
  | VString _ -> Type.TString
;;

let trans = function
  | Syntax.VInt n -> VInt n
  | Syntax.VString s -> VString s
;;

let minus_infty = function
  | Type.TInt -> VInt Int.min_int
  | Type.TString -> VString ""
;;

let plus_infty = function
  | Type.TInt -> VInt Int.max_int
  | Type.TString -> VString (String.make string_max_length '\xFF')
;;
