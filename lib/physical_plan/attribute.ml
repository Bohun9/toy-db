module C = Core

type t =
  { table_alias : string option
  ; column : string
  ; typ : C.Type.t
  }
[@@deriving show]

let show { table_alias; column; _ } =
  match table_alias with
  | Some alias -> Printf.sprintf "%s.%s" alias column
  | None -> column
;;

let of_table_field ({ table_alias; column; typ } : Logical_plan.Table_field.t) =
  { table_alias = Some table_alias; column; typ }
;;

let virtual_attr column typ = { table_alias = None; column; typ }
