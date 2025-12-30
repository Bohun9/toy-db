open Core
open Metadata
module Field = Field
module Table_field = Table_field

type table_expr =
  | Table of
      { name : string
      ; alias : string
      }
  | Join of
      { tab1 : table_expr
      ; tab2 : table_expr
      ; field1 : Table_field.t
      ; field2 : Table_field.t
      }

type predicate =
  { field : Table_field.t
  ; op : Syntax.relop
  ; value : Syntax.value
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
      { agg_kind : Syntax.aggregate_kind
      ; field : Table_field.t
      ; name : string
      ; result_type : Type.t
      }

type grouping =
  | NoGrouping of { select_list : select_list }
  | Grouping of
      { select_list : group_by_select_item list
      ; group_by_fields : Table_field.t list
      }

type order_item =
  { field : Field.t
  ; order : Syntax.order
  }

type t =
  | Select of
      { table_expr : table_expr
      ; predicates : (string, predicate list) Hashtbl.t
      ; grouping : grouping
      ; order : order_item list option
      }
  | InsertValues of
      { table : string
      ; tuples : Syntax.tuple list
      }

val build_plan : Table_registry.t -> Syntax.stmt -> t
