module C = Core
module M = Metadata

module Page_io = struct
  let page_size = 4096

  let read_page file page_no =
    let offset = page_no * page_size in
    In_channel.with_open_bin file (fun ic ->
      In_channel.seek ic (Int64.of_int offset);
      let data = Bytes.create page_size in
      (match In_channel.really_input ic data 0 (Bytes.length data) with
       | Some _ -> ()
       | None -> failwith "internal error - load_raw_page");
      data)
  ;;

  let write_page file page_no data =
    let len = Bytes.length data in
    assert (len <= page_size);
    let offset = page_no * page_size in
    Out_channel.with_open_gen [ Out_channel.Open_wronly ] 0o666 file (fun oc ->
      Out_channel.seek oc (Int64.of_int offset);
      Out_channel.output_bytes oc data;
      if len < page_size
      then Out_channel.output_bytes oc (Bytes.make (page_size - len) '\x00'))
  ;;

  let num_pages file =
    let len =
      In_channel.with_open_gen [ In_channel.Open_creat ] 0o666 file (fun ic ->
        Int64.to_int (In_channel.length ic))
    in
    assert (len mod page_size = 0);
    let num_pages = len / page_size in
    num_pages
  ;;
end

module Layout = struct
  let value_storage_size vt =
    match vt with
    | C.Type.Int -> C.Value.int_size
    | C.Type.String -> C.Value.string_max_length
  ;;

  let tuple_storage_size schema =
    M.Table_schema.types schema |> List.map value_storage_size |> List.fold_left ( + ) 0
  ;;
end

module Codec = struct
  let serialize_value b = function
    | C.Value.Int n -> Buffer.add_int64_le b (Int64.of_int n)
    | C.Value.String s ->
      assert (String.length s <= C.Value.string_max_length);
      let padded = s ^ String.make (C.Value.string_max_length - String.length s) '\x00' in
      Buffer.add_string b padded
  ;;

  let serialize_tuple b (t : C.Tuple.t) = List.iter (serialize_value b) t.attributes

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

  let deserialize_values c schema =
    M.Table_schema.types schema |> List.map (deserialize_value c)
  ;;

  let deserialize_tuple c schema page_no slot_idx =
    C.Tuple.
      { attributes = deserialize_values c schema
      ; rid = Some C.Record_id.{ page_no; slot_idx }
      }
  ;;
end
