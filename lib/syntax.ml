type relop = Eq

type field_name =
  | PureFieldName of string
  | QualifiedFieldName of
      { alias : string
      ; column : string
      }
[@@deriving show]

type value =
  | VInt of int
  | VString of string

type tuple = value list

let derive_value_type = function
  | VInt _ -> Type.TInt
  | VString _ -> Type.TString
;;

let derive_tuple_type = List.map derive_value_type

type select_exprs = Star

type table_expr =
  | Table of
      { name : string
      ; alias : string option
      }
  | Join of
      { tab1 : table_expr
      ; tab2 : table_expr
      ; field1 : field_name
      ; field2 : field_name
      }

type predicate =
  { field : field_name
  ; op : relop
  ; value : value
  }

type stmt =
  | Select of
      { exprs : select_exprs
      ; table_expr : table_expr
      ; predicates : predicate list
      }
  | InsertValues of
      { table : string
      ; tuples : tuple list
      }

type ddl =
  | CreateTable of string * Table_schema.t
  | DropTable of string

type sql =
  | SQL_Stmt of stmt
  | SQL_DDL of ddl
