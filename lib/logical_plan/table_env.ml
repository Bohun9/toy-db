open Core
open Metadata
module StringMap = Map.Make (String)

type t =
  { fields : Table_field.t list
  ; aliases : string list
  }

let empty = { fields = []; aliases = [] }

let extend_base env alias sch =
  { fields =
      env.fields
      @ List.map
          (fun Syntax.{ name; typ } ->
             Table_field.{ table_alias = alias; column = name; typ })
          (Table_schema.columns sch)
  ; aliases = env.aliases @ [ alias ]
  }
;;

let extend_derived env alias fields =
  { fields = env.fields @ fields; aliases = env.aliases @ [ alias ] }
;;

let merge env1 env2 =
  { fields = env1.fields @ env2.fields; aliases = env1.aliases @ env2.aliases }
;;

let resolve_field env fname =
  let matches = List.filter (Table_field.field_name_match fname) env.fields in
  match matches with
  | [] -> failwith "no matches"
  | [ field ] -> field
  | _ -> failwith "ambiguous field name"
;;

(* let resolve_pure_field env column = *)
(*   env *)
(*   |> StringMap.bindings *)
(*   |> List.filter_map (fun (alias, sch) -> *)
(*     Option.map *)
(*       (fun ({ typ; _ } : Table_schema.column_data) -> *)
(*          Table_field.{ table_alias = alias; column; typ }) *)
(*       (Table_schema.find_column sch column)) *)
(* ;; *)
(**)
(* let resolve_field { env; _ } = function *)
(*   | Syntax.PureFieldName column -> *)
(*     (match resolve_pure_field env column with *)
(*      | [] -> raise @@ Error.unknown_column column *)
(*      | [ resolved_field ] -> resolved_field *)
(*      | _ -> raise @@ Error.ambiguous_column column) *)
(*   | Syntax.QualifiedFieldName { alias; column } -> *)
(*     (match StringMap.find_opt alias env with *)
(*      | Some sch -> *)
(*        (match Table_schema.find_column sch column with *)
(*         | Some { typ; _ } -> { table_alias = alias; column; typ } *)
(*         | None -> raise @@ Error.unknown_column column) *)
(*      | None -> raise @@ Error.unbound_alias_name alias) *)
(* ;; *)

let alias_names { aliases; _ } = aliases
let fields { fields; _ } = fields
