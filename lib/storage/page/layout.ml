module C = Core
module M = Metadata

let page_size = 4096

let value_storage_size t =
  match t with
  | C.Type.Int -> C.Value.int_size
  | C.Type.String -> C.Value.string_max_length
;;

let tuple_storage_size schema =
  M.Table_schema.types schema |> List.map value_storage_size |> List.fold_left ( + ) 0
;;

let serialize_value b = function
  | C.Value.Int n -> Buffer.add_int64_le b (Int64.of_int n)
  | C.Value.String s ->
    assert (String.length s <= C.Value.string_max_length);
    let padded = s ^ String.make (C.Value.string_max_length - String.length s) '\x00' in
    Buffer.add_string b padded
;;

let serialize_tuple b t = List.iter (serialize_value b) (C.Tuple.attributes t)

let deserialize_value c vt =
  match vt with
  | C.Type.Int -> C.Value.Int (Int64.to_int (Cursor.read_int64_le c))
  | C.Type.String ->
    let raw = Cursor.read_string c C.Value.string_max_length in
    let s =
      match String.index_opt raw '\x00' with
      | Some i -> String.sub raw 0 i
      | None -> raw
    in
    C.Value.String s
;;

let deserialize_values c sch = M.Table_schema.types sch |> List.map (deserialize_value c)

let deserialize_tuple c sch page_no slot_idx =
  C.Tuple.create ?rid:(Some C.Record_id.{ page_no; slot_idx }) (deserialize_values c sch)
;;
