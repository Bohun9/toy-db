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

val build_plan : Table_registry.t -> Syntax.stmt -> t
