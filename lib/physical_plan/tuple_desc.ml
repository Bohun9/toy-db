open Core
open Metadata

type attribute =
  { table_alias : string
  ; column : string
  ; typ : Type.t
  }
[@@deriving show]

let attr_of_table_field ({ table_alias; column; typ } : Logical_plan.Table_field.t) =
  { table_alias; column; typ }
;;

type t = attribute list [@@deriving show]

let from_table_schema schema alias =
  List.map
    (fun ({ name; typ } : Table_schema.column_data) ->
       { table_alias = alias; column = name; typ })
    (Table_schema.columns schema)
;;

let from_table_fields table_fields = List.map attr_of_table_field table_fields

let from_grouping select_list =
  List.map
    (fun select_item ->
       match select_item with
       | Logical_plan.SelectField { field; _ } -> attr_of_table_field field
       | Logical_plan.SelectAggregate { agg_kind; field; name; result_type } ->
         { table_alias = ""; column = name; typ = result_type })
    select_list
;;

let dummy = []
let combine desc1 desc2 = desc1 @ desc2

let field_index desc (field : Logical_plan.Table_field.t) =
  desc
  |> List.find_index (fun f ->
    f.table_alias = field.table_alias && f.column = field.column)
  |> Option.get
;;
