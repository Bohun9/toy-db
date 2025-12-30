open Core
open Metadata
module StringMap = Map.Make (String)

type t =
  { env : Table_schema.t StringMap.t
  ; fields : Field.t list
  }

let empty = { env = StringMap.empty; fields = [] }

let extend { env; fields } alias sch =
  { env =
      (match StringMap.find_opt alias env with
       | Some _ -> raise @@ Error.duplicate_alias alias
       | None -> StringMap.add alias sch env)
  ; fields =
      fields
      @ List.map
          (fun Syntax.{ name; typ } ->
             Field.{ table_alias = Some alias; column = name; typ })
          (Table_schema.columns sch)
  }
;;

let merge env1 env2 =
  StringMap.fold (fun alias sch env -> extend env alias sch) env2.env env1
;;

let resolve_pure_field env column =
  env
  |> StringMap.bindings
  |> List.filter_map (fun (alias, sch) ->
    Option.map
      (fun ({ typ; _ } : Table_schema.column_data) ->
         Table_field.{ table_alias = alias; column; typ })
      (Table_schema.find_column sch column))
;;

let resolve_field { env; _ } = function
  | Syntax.PureFieldName column ->
    (match resolve_pure_field env column with
     | [] -> raise @@ Error.unknown_column column
     | [ resolved_field ] -> resolved_field
     | _ -> raise @@ Error.ambiguous_column column)
  | Syntax.QualifiedFieldName { alias; column } ->
    (match StringMap.find_opt alias env with
     | Some sch ->
       (match Table_schema.find_column sch column with
        | Some { typ; _ } -> { table_alias = alias; column; typ }
        | None -> raise @@ Error.unknown_column column)
     | None -> raise @@ Error.unbound_alias_name alias)
;;

let alias_names { env; _ } = env |> StringMap.bindings |> List.map fst
let fields { fields; _ } = fields
