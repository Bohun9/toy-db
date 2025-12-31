module C = Core
module M = Metadata
module L = Logical_plan

type t = Attribute.t list [@@deriving show]

let from_table_fields table_fields = List.map Attribute.of_table_field table_fields

let from_grouping select_list =
  List.map
    (fun select_item ->
       match select_item with
       | L.SelectField { field; _ } -> Attribute.of_table_field field
       | L.SelectAggregate { name; result_type; _ } ->
         Attribute.virtual_attr name result_type)
    select_list
;;

let dummy = []
let combine desc1 desc2 = desc1 @ desc2

let field_index desc (field : L.Field.t) =
  desc
  |> List.find_index (fun (a : Attribute.t) ->
    a.table_alias = field.table_alias && a.column = field.column)
  |> Option.get
;;

let table_field_index desc (field : L.Table_field.t) =
  field_index desc (L.Table_field.to_field field)
;;
