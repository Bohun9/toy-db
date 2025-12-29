type t =
  | EValue of Core.Value.t
  | EAttributeFetch of int

let of_field f desc = EAttributeFetch (Tuple_desc.field_index desc f)

let eval e t =
  match e with
  | EValue v -> v
  | EAttributeFetch i -> Core.Tuple.field t i
;;

let eval_exprs es t = List.map (fun e -> eval e t) es
