module C = Core
module M = Metadata

type t =
  { table_alias : string
  ; column : string
  ; typ : C.Type.t
  }

let to_field { table_alias; column; typ } =
  Field.{ table_alias = Some table_alias; column; typ }
;;

let of_field ({ table_alias; column; typ } : Field.t) =
  { table_alias = Option.get table_alias; column; typ }
;;

let of_field_with_alias alias ({ column; typ; _ } : Field.t) =
  { table_alias = alias; column; typ }
;;

let schema_to_fields table_alias sch =
  List.map
    (fun ({ name; typ } : C.Syntax.column_data) -> { table_alias; column = name; typ })
    (M.Table_schema.columns sch)
;;

type table_field = t

module type ENV = sig
  type 'a t

  val empty : 'a t
  val extend : 'a t -> table_field -> 'a -> 'a t
  val merge : 'a t -> 'a t -> 'a t
  val resolve_field : 'a t -> C.Syntax.field_name -> (table_field * 'a, exn) result
  val fields : 'a t -> table_field list
end

module Env : ENV = struct
  type 'a t = 'a Field.Env.t

  let empty = Field.Env.empty
  let extend env f data = Field.Env.extend env (to_field f) data
  let merge = Field.Env.merge

  let resolve_field env field_name =
    Field.Env.resolve_field env field_name
    |> Result.map (fun (f, data) -> of_field f, data)
  ;;

  let fields env = Field.Env.fields env |> List.map of_field
end
