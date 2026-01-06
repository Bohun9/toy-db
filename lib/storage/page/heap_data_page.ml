module C = Core
module M = Metadata

type page_data =
  { slots : C.Tuple.t option array
  ; mutable num_tuples : int
  }
[@@deriving show]

type t = page_data Generic_page.t

let slots (p : t) = p.data.slots
let num_tuples (p : t) = p.data.num_tuples
let num_slots p = Array.length (slots p)

let fill_slot (p : t) i t =
  assert (Option.is_none @@ (slots p).(i));
  p.data.slots.(i) <- Some t;
  p.data.num_tuples <- p.data.num_tuples + 1;
  Generic_page.set_dirty p
;;

let free_slot (p : t) i =
  assert (Option.is_some @@ (slots p).(i));
  p.data.slots.(i) <- None;
  p.data.num_tuples <- p.data.num_tuples - 1;
  Generic_page.set_dirty p
;;

let has_empty_slot p = num_tuples p < num_slots p
let page_header_size = 2
let num_slots sch = (Layout.page_size - page_header_size) / Layout.tuple_storage_size sch

let create page_no sch =
  Generic_page.create page_no
  @@ { slots = Array.make (num_slots sch) None; num_tuples = 0 }
;;

let serialize p =
  let b = Buffer.create Layout.page_size in
  Buffer.add_int16_le b (num_tuples p);
  Array.iter (Option.iter (Layout.serialize_tuple b)) (slots p);
  Buffer.to_bytes b
;;

let deserialize page_no sch data =
  let c = Cursor.create data in
  let num_tuples = Cursor.read_int16_le c in
  let slots =
    Array.init (num_slots sch) (fun i ->
      if i < num_tuples then Some (Layout.deserialize_tuple c sch page_no i) else None)
  in
  Generic_page.create page_no @@ { slots; num_tuples }
;;

let insert_tuple p t =
  let slot_idx = Array.find_index Option.is_none (slots p) |> Option.get in
  let rid = Some C.Record_id.{ page_no = p.page_no; slot_idx } in
  fill_slot p slot_idx { t with rid }
;;

let delete_tuple p (rid : C.Record_id.t) = free_slot p rid.slot_idx
let scan_page p = slots p |> Array.to_seq |> Seq.filter_map Fun.id
