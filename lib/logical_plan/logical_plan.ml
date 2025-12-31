module C = Core
module M = Metadata
module Field = Field
module Table_field = Table_field

type predicate =
  { field : Table_field.t
  ; op : C.Syntax.relop
  ; value : C.Syntax.value
  }

type select_list =
  | Star
  | SelectFields of Table_field.t list

type group_by_select_item =
  | SelectField of
      { field : Table_field.t
      ; group_by_index : int
      }
  | SelectAggregate of
      { agg_kind : C.Syntax.aggregate_kind
      ; field : Table_field.t
      ; name : string
      ; result_type : C.Type.t
      }

type grouping =
  | NoGrouping of { select_list : select_list }
  | Grouping of
      { select_list : group_by_select_item list
      ; group_by_fields : Table_field.t list
      }

type order_specifier =
  { field : Field.t
  ; order : C.Syntax.order
  }

type table_expr =
  | Table of
      { name : string
      ; alias : string
      }
  | Subquery of
      { select : select_stmt
      ; fields : Table_field.t list
      }
  | Join of
      { tab1 : table_expr
      ; tab2 : table_expr
      ; field1 : Table_field.t
      ; field2 : Table_field.t
      }

and select_stmt =
  { table_expr : table_expr
  ; predicates : (string, predicate list) Hashtbl.t
  ; grouping : grouping
  ; order_specifiers : order_specifier list option
  ; limit : int option
  ; offset : int option
  }

type t =
  | Select of select_stmt
  | InsertValues of
      { table : string
      ; tuples : C.Syntax.tuple list
      }

let agg_result_type agg_kind input_type =
  match agg_kind with
  | C.Syntax.Count -> C.Type.Int
  | C.Syntax.Sum -> C.Type.Int
  | C.Syntax.Avg -> C.Type.Int
  | C.Syntax.Min -> input_type
  | C.Syntax.Max -> input_type
;;

let require_same_type t1 t2 = if t1 <> t2 then raise Error.type_mismatch

let get_table reg name =
  match M.Table_registry.get_table_opt reg name with
  | Some t -> t
  | None -> raise Error.table_not_found
;;

let get_table_schema reg name = get_table reg name |> M.Db_file.schema

let rec check_table_expr reg = function
  | C.Syntax.Table { name; alias } ->
    let sch = get_table_schema reg name in
    let alias = Option.value alias ~default:name in
    Table { name; alias }, Table_env.extend_base Table_env.empty alias sch
  | C.Syntax.Subquery { select; alias } ->
    let select, select_list_fields = build_plan_select reg select in
    let fields = List.map (Field.to_table_field alias) select_list_fields in
    Subquery { select; fields }, Table_env.extend_derived Table_env.empty alias fields
  | C.Syntax.Join { tab1; tab2; field1 = field_name1; field2 = field_name2 } ->
    let tab1, env1 = check_table_expr reg tab1 in
    let tab2, env2 = check_table_expr reg tab2 in
    let field1 = Table_env.resolve_field env1 field_name1 in
    let field2 = Table_env.resolve_field env2 field_name2 in
    require_same_type field1.typ field2.typ;
    Join { tab1; tab2; field1; field2 }, Table_env.merge env1 env2

and build_plan_select
      reg
      C.Syntax.{ select_list; table_expr; predicates; group_by; order_by; limit; offset }
  =
  let table_expr, env = check_table_expr reg table_expr in
  let grouped_predicates = Hashtbl.create 16 in
  List.iter
    (fun alias -> Hashtbl.add grouped_predicates alias [])
    (Table_env.alias_names env);
  List.iter
    (fun ({ field = field_name; op; value } : C.Syntax.predicate) ->
       let field = Table_env.resolve_field env field_name in
       require_same_type field.typ (C.Syntax.derive_value_type value);
       Hashtbl.replace
         grouped_predicates
         field.table_alias
         ({ field; op; value } :: Hashtbl.find grouped_predicates field.table_alias))
    predicates;
  let grouping, select_list_fields =
    match group_by with
    | None ->
      let select_list =
        match select_list with
        | C.Syntax.Star -> Star
        | C.Syntax.SelectList select_list ->
          SelectFields
            (List.map
               (fun select_item ->
                  match select_item with
                  | C.Syntax.SelectField { field } -> Table_env.resolve_field env field
                  | C.Syntax.SelectAggregate _ -> raise Error.aggregate_without_grouping)
               select_list)
      in
      let select_list_fields =
        match select_list with
        | Star -> List.map Field.of_table_field (Table_env.fields env)
        | SelectFields fields -> List.map Field.of_table_field fields
      in
      NoGrouping { select_list }, select_list_fields
    | Some { group_by_fields } ->
      let group_by_fields = List.map (Table_env.resolve_field env) group_by_fields in
      let group_by_seq = Table_field.Seq.from_list group_by_fields in
      let select_list =
        match select_list with
        | C.Syntax.Star -> failwith "star with grouping"
        | C.Syntax.SelectList select_list ->
          List.map
            (fun select_item ->
               match select_item with
               | C.Syntax.SelectField { field } ->
                 let group_by_index, field =
                   Table_field.Seq.resolve_field group_by_seq field
                 in
                 SelectField { field; group_by_index }
               | C.Syntax.SelectAggregate { agg_kind; field = field_name; name } ->
                 let field = Table_env.resolve_field env field_name in
                 SelectAggregate
                   { agg_kind
                   ; field
                   ; name
                   ; result_type = agg_result_type agg_kind field.typ
                   })
            select_list
      in
      let select_list_fields =
        List.map
          (fun select_item ->
             match select_item with
             | SelectField { field; _ } -> Field.of_table_field field
             | SelectAggregate { name; result_type; _ } ->
               Field.virtual_field name result_type)
          select_list
      in
      Grouping { select_list; group_by_fields }, select_list_fields
  in
  let select_list_env = Field.Env.from_list select_list_fields in
  let order_specifiers =
    Option.map
      (fun order_list ->
         List.map
           (fun ({ field = field_name; order } : C.Syntax.order_specifier) ->
              { field = Field.Env.resolve_field select_list_env field_name
              ; order = Option.value order ~default:C.Syntax.Asc
              })
           order_list)
      order_by
  in
  ( { table_expr
    ; predicates = grouped_predicates
    ; grouping
    ; order_specifiers
    ; limit
    ; offset
    }
  , select_list_fields )
;;

let build_plan reg = function
  | C.Syntax.Select select -> Select (build_plan_select reg select |> fst)
  | C.Syntax.InsertValues { table; tuples } ->
    let sch = get_table_schema reg table in
    let tuple_types = List.map C.Syntax.derive_tuple_type tuples in
    if List.for_all (M.Table_schema.typecheck sch) tuple_types
    then InsertValues { table; tuples }
    else raise Error.type_mismatch
;;
