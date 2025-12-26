let page_size = 4096

let load_raw_page file page_no =
  Log.log "loading raw page %d from %s" page_no file;
  let offset = page_no * page_size in
  In_channel.with_open_bin file (fun ic ->
    In_channel.seek ic (Int64.of_int offset);
    let data = Bytes.create page_size in
    (match In_channel.really_input ic data 0 (Bytes.length data) with
     | Some _ -> ()
     | None -> failwith "internal error - load_raw_page");
    data)
;;

let flush_raw_page file page_no data =
  Log.log "flushing raw page %d to %s" page_no file;
  let len = Bytes.length data in
  assert (len <= page_size);
  let offset = page_no * page_size in
  Out_channel.with_open_gen [ Out_channel.Open_wronly ] 0o666 file (fun oc ->
    Out_channel.seek oc (Int64.of_int offset);
    Out_channel.output_bytes oc data;
    if len < page_size
    then Out_channel.output_bytes oc (Bytes.make (page_size - len) '\x00'))
;;

let get_num_pages file =
  let len =
    In_channel.with_open_gen [ In_channel.Open_creat ] 0o666 file (fun ic ->
      Int64.to_int (In_channel.length ic))
  in
  assert (len mod page_size = 0);
  let num_pages = len / page_size in
  num_pages
;;

let value_storage_size vt =
  match vt with
  | Type.TInt -> Value.int_size
  | Type.TString -> Value.string_max_length
;;

let tuple_storage_size schema =
  let sizes = List.map value_storage_size (Table_schema.types schema) in
  List.fold_left ( + ) 0 sizes
;;

let serialize_value b = function
  | Value.VInt n -> Buffer.add_int64_le b (Int64.of_int n)
  | Value.VString s ->
    assert (String.length s <= Value.string_max_length);
    let padded = s ^ String.make (Value.string_max_length - String.length s) '\x00' in
    Buffer.add_string b padded
;;

let serialize_tuple b (t : Tuple.t) = List.iter (serialize_value b) t.values

let deserialize_value c vt =
  match vt with
  | Type.TInt -> Value.VInt (Int64.to_int (Cursor.read_int64_le c))
  | Type.TString ->
    let raw = Cursor.read_string c Value.string_max_length in
    let s =
      match String.index_opt raw '\x00' with
      | Some i -> String.sub raw 0 i
      | None -> raw
    in
    Value.VString s
;;

let deserialize_values c schema =
  Table_schema.types schema |> List.map (deserialize_value c)
;;

let deserialize_tuple c schema page_no slot_idx : Tuple.t =
  { values = deserialize_values c schema
  ; rid = Some (Tuple.RecordID { page_no; slot_idx })
  }
;;

module type DBFILE = sig
  type t

  val insert_tuple : t -> Tuple.t -> Transaction_id.t -> unit
  val scan_file : t -> Transaction_id.t -> Tuple.t Seq.t
  val schema : t -> Table_schema.t
end
