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

type field_metadata =
  | FieldMetadata of
      { name : Syntax.field_name
      ; typ : value_type
      }
[@@deriving show]

let field_metadata name typ = FieldMetadata { name = Syntax.PureFieldName name; typ }

type tuple_descriptor = field_metadata list [@@deriving show]

type value =
  | VInt of int
  | VBool of bool
  | VString of string
[@@deriving show]

let value_lt v1 v2 =
  match v1, v2 with
  | VInt n1, VInt n2 -> n1 < n2
  | VBool b1, VBool b2 -> b1 < b2
  | VString s1, VString s2 -> s1 < s2
  | _ -> failwith "internal error - value_lt"
;;

let value_to_int = function
  | VInt n -> n
  | _ -> failwith "internal error - value_to_int"
;;

let value_to_string = function
  | VString s -> s
  | _ -> failwith "internal error -  value_to_string"
;;

type t =
  { desc : tuple_descriptor
  ; values : value list
  ; rid : record_id option
  }
[@@deriving show]

type tuples = t list [@@deriving show]

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

let set_tuple_alias alias t = { t with desc = set_desc_alias t.desc alias }
let set_tuple_desc t desc = { t with desc }

let combine_tuple t1 t2 =
  { desc = t1.desc @ t2.desc; values = t1.values @ t2.values; rid = None }
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
  { desc = derive_desc values; values; rid = None }
;;

let extract_types desc = List.map (fun (FieldMetadata { typ; _ }) -> typ) desc
let match_desc desc1 desc2 = extract_types desc1 = extract_types desc2
let field t n = List.nth t.values n

let field_type desc n =
  let (FieldMetadata { typ; _ }) = List.nth desc n in
  typ
;;
