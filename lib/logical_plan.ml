type resolved_field =
  { table_alias : string
  ; column : string
  }

type table_expr =
  | Table of
      { name : string
      ; alias : string
      }
  | Join of
      { tab1 : table_expr
      ; tab2 : table_expr
      ; field1 : resolved_field
      ; field2 : resolved_field
      }

type predicate =
  { column : string
  ; op : Syntax.relop
  ; value : Syntax.value
  }

type t =
  | Select of
      { table_expr : table_expr
      ; predicates : (string, predicate list) Hashtbl.t
      }
  | InsertValues of
      { table : string
      ; tuples : Syntax.tuple list
      }

module type ENV = sig
  type t

  val empty : t
  val extend : string -> Table_schema.t -> t -> t
  val merge : t -> t -> t
  val resolve_field : t -> Syntax.field_name -> resolved_field * Type.t
  val alias_names : t -> string list
end

module Env : ENV = struct
  module StringMap = Map.Make (String)

  type t = Table_schema.t StringMap.t

  let empty = StringMap.empty

  let extend alias sch env =
    match StringMap.find_opt alias env with
    | Some _ -> raise @@ Error.duplicate_alias alias
    | None -> StringMap.add alias sch env
  ;;

  let merge = StringMap.fold extend

  let resolve_pure_field env column =
    env
    |> StringMap.bindings
    |> List.filter_map (fun (alias, sch) ->
      Option.map
        (fun ({ typ; _ } : Table_schema.column_data) ->
           { table_alias = alias; column }, typ)
        (Table_schema.find_column sch column))
  ;;

  let resolve_field env = function
    | Syntax.PureFieldName column ->
      (match resolve_pure_field env column with
       | [] -> raise @@ Error.unknown_column column
       | [ resolved_field ] -> resolved_field
       | _ -> raise @@ Error.ambiguous_column column)
    | Syntax.QualifiedFieldName { alias; column } ->
      (match StringMap.find_opt alias env with
       | Some sch ->
         (match Table_schema.find_column sch column with
          | Some { typ; _ } -> { table_alias = alias; column }, typ
          | None -> raise @@ Error.unknown_column column)
       | None -> raise @@ Error.unbound_alias_name alias)
  ;;

  let alias_names env = env |> StringMap.bindings |> List.map fst
end

let require_same_type t1 t2 = if t1 <> t2 then raise Error.type_mismatch

let get_table reg name =
  match Table_registry.get_table_opt reg name with
  | Some t -> t
  | None -> raise Error.table_not_found
;;

let get_table_schema reg name = get_table reg name |> Packed_dbfile.schema

let rec check_table_expr reg = function
  | Syntax.Table { name; alias } ->
    let sch = get_table_schema reg name in
    let alias = Option.value alias ~default:name in
    Table { name; alias }, Env.extend alias sch Env.empty
  | Syntax.Join { tab1; tab2; field1; field2 } ->
    let tab1, env1 = check_table_expr reg tab1 in
    let tab2, env2 = check_table_expr reg tab2 in
    let rfield1, t1 = Env.resolve_field env1 field1 in
    let rfield2, t2 = Env.resolve_field env2 field2 in
    require_same_type t1 t2;
    Join { tab1; tab2; field1 = rfield1; field2 = rfield2 }, Env.merge env1 env2
;;

let build_plan reg = function
  | Syntax.Select { table_expr; predicates; _ } ->
    let table_expr, env = check_table_expr reg table_expr in
    let grouped_predicates = Hashtbl.create 16 in
    List.iter (fun alias -> Hashtbl.add grouped_predicates alias []) (Env.alias_names env);
    List.iter
      (fun ({ field; op; value } : Syntax.predicate) ->
         let { table_alias; column }, t = Env.resolve_field env field in
         require_same_type t (Syntax.derive_value_type value);
         Hashtbl.replace
           grouped_predicates
           table_alias
           ({ column; op; value } :: Hashtbl.find grouped_predicates table_alias))
      predicates;
    Select { table_expr; predicates = grouped_predicates }
  | Syntax.InsertValues { table; tuples } ->
    let sch = get_table_schema reg table in
    let tuple_types = List.map Syntax.derive_tuple_type tuples in
    if List.for_all (Table_schema.typecheck sch) tuple_types
    then InsertValues { table; tuples }
    else raise Error.type_mismatch
;;
