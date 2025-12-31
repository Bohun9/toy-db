type t =
  | Value of Core.Value.t
  | AttributeFetch of int

let of_table_field f desc = AttributeFetch (Tuple_desc.table_field_index desc f)
let of_field f desc = AttributeFetch (Tuple_desc.field_index desc f)

let eval e t =
  match e with
  | Value v -> v
  | AttributeFetch i -> Core.Tuple.attribute t i
;;
