module Field = Field
module Table_field = Table_field

type predicate =
  { field : Table_field.t
  ; op : Core.Syntax.relop
  ; value : Core.Syntax.value
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
      { agg_kind : Core.Syntax.aggregate_kind
      ; field : Table_field.t
      ; name : string
      ; result_type : Core.Type.t
      }

type grouping =
  | NoGrouping of { select_list : select_list }
  | Grouping of
      { select_list : group_by_select_item list
      ; group_by_fields : Table_field.t list
      }

type order_specifier =
  { field : Field.t
  ; order : Core.Syntax.order
  }

type table_expr =
  | Table of
      { name : string
      ; alias : string
      ; fields : Table_field.t list
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
      ; tuples : Core.Syntax.tuple list
      }

val build_plan : Metadata.Table_registry.t -> Core.Syntax.stmt -> t
