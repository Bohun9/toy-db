type binop =
  | Eq
  | Neq
  | Add

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

type expr =
  | EValue of value
  | EField of field_name
  | EBinop of expr * binop * expr

type select_exprs = Star

type table_expr =
  | Table of
      { name : string
      ; alias : string option
      }
  | Join of
      { tab1 : table_expr
      ; tab2 : table_expr
      ; e1 : expr
      ; e2 : expr
      }

type dml =
  | Select of
      { exprs : select_exprs
      ; table_expr : table_expr
      ; predicates : expr list
      }
  | InsertValues of
      { table : string
      ; tuples : tuple list
      }

type column_type =
  | TInt
  | TString

type column_data =
  | ColumnData of
      { name : string
      ; typ : column_type
      }

type table_schema = column_data list

type ddl =
  | CreateTable of string * table_schema
  | DropTable of string

type sql =
  | SQL_DML of dml
  | SQL_DDL of ddl
