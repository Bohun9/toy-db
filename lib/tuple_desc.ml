type attribute =
  { table_alias : string
  ; column : string
  ; typ : Type.t
  }
[@@deriving show]

type t = attribute list [@@deriving show]

let from_table_schema schema alias =
  List.map
    (fun ({ name; typ } : Table_schema.column_data) ->
       { table_alias = alias; column = name; typ })
    (Table_schema.columns schema)
;;

let dummy = []
let combine desc1 desc2 = desc1 @ desc2

let field_index desc (field : Logical_plan.resolved_field) =
  desc
  |> List.find_index (fun f ->
    f.table_alias = field.table_alias && f.column = field.column)
  |> Option.get
;;
