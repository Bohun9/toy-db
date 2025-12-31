module C = Core

type t =
  { table_alias : string option
  ; column : string
  ; typ : C.Type.t
  }
[@@deriving show]

let of_table_field ({ table_alias; column; typ } : Logical_plan.Table_field.t) =
  { table_alias = Some table_alias; column; typ }
;;

let virtual_attr column typ = { table_alias = None; column; typ }
