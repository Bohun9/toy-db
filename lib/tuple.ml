let int_size = 8
let string_max_length = 32

type record_id =
  | RecordID of
      { page_no : int
      ; slot_idx : int
      }
[@@deriving show]

type value_type =
  | TInt
  | TBool
  | TString
[@@deriving show]

(* type field_name = *)
(*   | QualifiedFieldName of *)
(*       { alias : string *)
(*       ; column : string *)
(*       } *)
(*   | PureFieldName of string *)
(* [@@deriving show] *)

type field_metadata =
  | FieldMetadata of
      { name : Syntax.field_name
      ; typ : value_type
      }
[@@deriving show]

type tuple_descriptor = field_metadata list [@@deriving show]

type value =
  | VInt of int
  | VBool of bool
  | VString of string
[@@deriving show]

type tuple =
  | Tuple of
      { desc : tuple_descriptor
      ; values : value list
      ; rid : record_id option
      }
[@@deriving show]

type tuples = tuple list [@@deriving show]

let trans_column_type = function
  | Syntax.TInt -> TInt
  | Syntax.TString -> TString
;;

let trans_column_data (Syntax.ColumnData { name; typ }) =
  FieldMetadata { name = Syntax.PureFieldName name; typ = trans_column_type typ }
;;

let trans_table_schema schema = List.map trans_column_data schema

let set_field_alias name alias =
  match name with
  | Syntax.QualifiedFieldName { column; _ } -> Syntax.QualifiedFieldName { alias; column }
  | Syntax.PureFieldName column -> Syntax.QualifiedFieldName { alias; column }
;;

let set_desc_alias desc alias =
  List.map
    (fun (FieldMetadata fm) ->
      FieldMetadata { fm with name = set_field_alias fm.name alias })
    desc
;;

let set_tuple_alias alias (Tuple t) = Tuple { t with desc = set_desc_alias t.desc alias }

let combine_tuple (Tuple t1) (Tuple t2) =
  Tuple { desc = t1.desc @ t2.desc; values = t1.values @ t2.values; rid = None }
;;

let derive_type = function
  | VInt _ -> TInt
  | VBool _ -> TBool
  | VString _ -> TString
;;

let derive_desc vs =
  List.map (fun v -> FieldMetadata { name = PureFieldName "_"; typ = derive_type v }) vs
;;

let trans_value (v : Syntax.value) =
  match v with
  | Syntax.VInt n -> VInt n
  | Syntax.VString s -> VString s
;;

let trans_tuple vs =
  let values = List.map trans_value vs in
  Tuple { desc = derive_desc values; values; rid = None }
;;

let extract_types desc = List.map (fun (FieldMetadata { typ; _ }) -> typ) desc
let match_desc desc1 desc2 = extract_types desc1 = extract_types desc2
