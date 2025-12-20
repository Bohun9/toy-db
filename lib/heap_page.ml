let page_size = 4096
let page_header_size = 2

type t =
  { page_no : int
  ; desc : Tuple.tuple_descriptor
  ; slots : Tuple.t option array
  ; mutable num_tuples : int
  ; mutable dirty : bool
  }
[@@deriving show]

let is_dirty hp = hp.dirty
let set_dirty hp = hp.dirty <- true
let clear_dirty hp = hp.dirty <- false

let value_storage_size vt =
  match vt with
  | Tuple.TInt -> Tuple.int_size
  | Tuple.TString -> Tuple.string_max_length
  | Tuple.TBool -> failwith "internal error - value_storage_size"
;;

let tuple_storage_size desc =
  let types = List.map (fun (Tuple.FieldMetadata { typ; _ }) -> typ) desc in
  let sizes = List.map value_storage_size types in
  List.fold_left ( + ) 0 sizes
;;

let num_slots desc = (page_size - page_header_size) / tuple_storage_size desc

let create page_no desc =
  { page_no
  ; desc
  ; slots = Array.make (num_slots desc) None
  ; num_tuples = 0
  ; dirty = false
  }
;;

let serialize_value b = function
  | Tuple.VInt n -> Buffer.add_int64_le b (Int64.of_int n)
  | Tuple.VString s ->
    assert (String.length s <= Tuple.string_max_length);
    let padded = s ^ String.make (Tuple.string_max_length - String.length s) '\x00' in
    Buffer.add_string b padded
  | Tuple.VBool _ -> failwith "internal error - serialize_value"
;;

let serialize_tuple b (t : Tuple.t) = List.iter (serialize_value b) t.values

let serialize hp =
  let b = Buffer.create page_size in
  Buffer.add_int16_le b hp.num_tuples;
  Array.iter (Option.iter (serialize_tuple b)) hp.slots;
  Buffer.add_bytes b (Bytes.create (page_size - Buffer.length b));
  Buffer.to_bytes b
;;

let deserialize_value c vt =
  match vt with
  | Tuple.TInt -> Tuple.VInt (Int64.to_int (Cursor.read_int64_le c))
  | Tuple.TString ->
    let raw = Cursor.read_string c Tuple.string_max_length in
    let s =
      match String.index_opt raw '\x00' with
      | Some i -> String.sub raw 0 i
      | None -> raw
    in
    Tuple.VString s
  | Tuple.TBool -> failwith "internal error - deserialize_value"
;;

let deserialize_values c desc =
  List.map (fun (Tuple.FieldMetadata { typ; _ }) -> deserialize_value c typ) desc
;;

let deserialize page_no desc data =
  let c = Cursor.create data in
  let num_tuples = Cursor.read_int16_le c in
  let deserialize_tuple slot_idx : Tuple.t =
    { desc
    ; values = deserialize_values c desc
    ; rid = Some (Tuple.RecordID { page_no; slot_idx })
    }
  in
  let slots =
    Array.init (num_slots desc) (fun i ->
      if i < num_tuples then Some (deserialize_tuple i) else None)
  in
  { page_no; desc; slots; num_tuples; dirty = false }
;;

let insert_tuple hp t =
  match Array.find_index Option.is_none hp.slots with
  | None -> false
  | Some slot_idx ->
    let rid = Some (Tuple.RecordID { page_no = hp.page_no; slot_idx }) in
    hp.slots.(slot_idx) <- Some { t with rid };
    hp.num_tuples <- hp.num_tuples + 1;
    set_dirty hp;
    true
;;

let delete_tuple hp (Tuple.RecordID rid) =
  match hp.slots.(rid.slot_idx) with
  | Some _ ->
    hp.slots.(rid.slot_idx) <- None;
    hp.num_tuples <- hp.num_tuples - 1;
    set_dirty hp
  | None -> failwith "internal error - delete_tuple"
;;

let scan_page hp = hp.slots |> Array.to_seq |> Seq.filter_map Fun.id
