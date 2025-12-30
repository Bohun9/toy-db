type relop =
  | Eq
  | Le
  | Lt
  | Ge
  | Gt

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

type aggregate_kind =
  | Count
  | Sum
  | Avg
  | Min
  | Max

type select_item =
  | SelectField of { field : field_name }
  | SelectAggregate of
      { agg_kind : aggregate_kind
      ; field : field_name
      ; name : string
      }

type select_list =
  | Star
  | SelectList of select_item list

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

type group_by = { group_by_fields : field_name list }

type order =
  | Asc
  | Desc

type order_item =
  { field : field_name
  ; order : order option
  }

type stmt =
  | Select of
      { select_list : select_list
      ; table_expr : table_expr
      ; predicates : predicate list
      ; group_by : group_by option
      ; order_by : order_item list option
      ; limit : int option
      ; offset : int option
      }
  | InsertValues of
      { table : string
      ; tuples : tuple list
      }

type column_data =
  { name : string
  ; typ : Type.t
  }
[@@deriving show]

type table_schema =
  { columns : column_data list
  ; primary_key : string option
  }

type ddl =
  | CreateTable of string * table_schema
  | DropTable of string

type sql =
  | SQL_Stmt of stmt
  | SQL_DDL of ddl
