let int_size = 8
let string_max_length = 32

type t =
  | VInt of int
  | VString of string
[@@deriving show]

let value_eval_relop v1 op v2 =
  match op with
  | Syntax.Eq -> v1 = v2
;;

let value_lt v1 v2 =
  match v1, v2 with
  | VInt n1, VInt n2 -> n1 < n2
  | VString s1, VString s2 -> s1 < s2
  | _ -> failwith "internal error - value_lt"
;;

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
