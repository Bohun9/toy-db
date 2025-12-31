type t =
  { table_alias : string option (* [None] for virtual columns. *)
  ; column : string
  ; typ : Core.Type.t
  }

let virtual_field column typ = { table_alias = None; column; typ }

let of_table_field ({ table_alias; column; typ } : Table_field.t) =
  { table_alias = Some table_alias; column; typ }
;;

let to_table_field alias { column; typ; _ } =
  Table_field.{ table_alias = alias; column; typ }
;;

let field_name_match fname field =
  match fname with
  | Core.Syntax.UnqualifiedField { column } -> column = field.column
  | Core.Syntax.QualifiedField { alias; column } ->
    Some alias = field.table_alias && column = field.column
;;

type field = t

module type ENV = sig
  type t

  val from_list : field list -> t
  val resolve_field : t -> Core.Syntax.field_name -> field
end

module Env : ENV = struct
  type t = field list

  let from_list fs = fs

  let resolve_field fs field_name =
    let matches = List.filter (fun f -> field_name_match field_name f) fs in
    match matches with
    | [] -> failwith "no match"
    | [ field ] -> field
    | _ -> failwith "ambiguous field name"
  ;;
end
