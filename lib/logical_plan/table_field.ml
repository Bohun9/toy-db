type t =
  { table_alias : string
  ; column : string
  ; typ : Core.Type.t
  }

let field_name_match fname tfield =
  match fname with
  | Core.Syntax.PureFieldName column -> column = tfield.column
  | Core.Syntax.QualifiedFieldName { alias; column } ->
    alias = tfield.table_alias && column = tfield.column
;;

type table_field = t

module type SEQ = sig
  type t

  val from_list : table_field list -> t
  val resolve_field : t -> Core.Syntax.field_name -> int * table_field
end

module Seq : SEQ = struct
  type t = table_field list

  let from_list fs = fs

  let resolve_field fs field_name =
    let indexed_fs = List.mapi (fun i f -> i, f) fs in
    let matches = List.filter (fun (_, f) -> field_name_match field_name f) indexed_fs in
    let unique_matches = List.sort_uniq (fun (_, f1) (_, f2) -> compare f1 f2) matches in
    match unique_matches with
    | [] -> failwith "no match"
    | [ ifield ] -> ifield
    | _ -> failwith "ambiguous field name"
  ;;
end
