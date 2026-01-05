module C = Core

let leaf_id = 'L'

type leaf_data =
  { key_attribute : int
  ; tuples : C.Tuple.t Array.t
  ; mutable num_tuples : int
  ; mutable next_leaf : int option
  }

type t = leaf_data Generic_page.t Btree_node.t

let key_attribute (p : t) = p.data.data.key_attribute
let tuples (p : t) = p.data.data.tuples
let num_tuples (p : t) = p.data.data.num_tuples
let next_leaf (p : t) = p.data.data.next_leaf

let set_next_leaf (p : t) v =
  p.data.data.next_leaf <- v;
  Btree_node.set_dirty p
;;

let set_num_tuples (p : t) v =
  p.data.data.num_tuples <- v;
  Btree_node.set_dirty p
;;

let max_num_tuples p = Array.length (tuples p) - 1
let min_num_tuples p = (max_num_tuples p + 1) / 2
let key_of_tuple p t = C.Tuple.attribute t (key_attribute p)
let tuple p i = (tuples p).(i)
let lowest_key p = key_of_tuple p (tuple p 0)

let max_num_tuples_for_schema sch =
  (Storage_layout.Page_io.page_size - (1 + 8 + 8 + 2))
  / Storage_layout.Layout.tuple_storage_size sch
;;

let create_storage sch =
  Array.make (max_num_tuples_for_schema sch + 1) (C.Tuple.create [])
;;

type tuples_info =
  { tuples : C.Tuple.t Array.t
  ; num_tuples : int
  }

let create page_no sch key_attribute parent tuples_info next_leaf =
  let tuples, num_tuples =
    match tuples_info with
    | Some { tuples; num_tuples } -> tuples, num_tuples
    | None -> create_storage sch, 0
  in
  Btree_node.create parent
  @@ Generic_page.create page_no { key_attribute; tuples; num_tuples; next_leaf }
;;

let encode_next_leaf p = Option.value (next_leaf p) ~default:0

let decode_next_leaf p =
  match p with
  | 0 -> None
  | p -> Some p
;;

let serialize p =
  let b = Buffer.create Storage_layout.Page_io.page_size in
  Buffer.add_char b leaf_id;
  Buffer.add_int64_le
    b
    (Btree_node.parent_opt p |> Btree_node.encode_parent |> Int64.of_int);
  Buffer.add_int64_le b (encode_next_leaf p |> Int64.of_int);
  Buffer.add_int16_le b (num_tuples p);
  for i = 0 to num_tuples p - 1 do
    Storage_layout.Codec.serialize_tuple b (tuple p i)
  done;
  Buffer.to_bytes b
;;

let deserialize page_no sch key_attribute data =
  let c = Cursor.create data in
  assert (Cursor.read_char c = leaf_id);
  let parent = Cursor.read_int64_le c |> Int64.to_int |> Btree_node.decode_parent in
  let next_leaf = Cursor.read_int64_le c |> Int64.to_int |> decode_next_leaf in
  let num_tuples = Cursor.read_int16_le c in
  let tuples = create_storage sch in
  for i = 0 to num_tuples - 1 do
    tuples.(i) <- Storage_layout.Codec.deserialize_tuple c sch page_no i
  done;
  create page_no sch key_attribute parent (Some { tuples; num_tuples }) next_leaf
;;

type insert_result =
  | Inserted
  | Split of tuples_info

let find_key_pos p k =
  let rec loop i =
    if i = num_tuples p || C.Value.eval_le k (key_of_tuple p (tuple p i))
    then i
    else loop (i + 1)
  in
  loop 0
;;

let shift_tuples_right p from =
  let len = num_tuples p - from in
  Array.blit (tuples p) from (tuples p) (from + 1) len
;;

let shift_tuples_left p from =
  let len = num_tuples p - from in
  Array.blit (tuples p) from (tuples p) (from - 1) len
;;

let insert_tuple p t =
  let key = key_of_tuple p t in
  let pos = find_key_pos p key in
  let tuples = tuples p in
  shift_tuples_right p pos;
  tuples.(pos) <- t;
  set_num_tuples p (num_tuples p + 1);
  if num_tuples p <= max_num_tuples p
  then Inserted
  else (
    let size1 = num_tuples p / 2 in
    let size2 = num_tuples p - size1 in
    let tuples2 = Array.copy tuples in
    Array.blit tuples size1 tuples2 0 size2;
    set_num_tuples p size1;
    Split { tuples = tuples2; num_tuples = size2 })
;;

let insert_tuple_no_split p t =
  match insert_tuple p t with
  | Inserted -> ()
  | Split _ -> failwith "internal error"
;;

type delete_result =
  | Deleted
  | Underfull

let can_coalesce p1 p2 =
  assert (max_num_tuples p1 = max_num_tuples p2);
  num_tuples p1 + num_tuples p2 <= max_num_tuples p1
;;

let coalesce p1 p2 =
  Array.blit (tuples p2) 0 (tuples p1) (num_tuples p1) (num_tuples p2);
  set_num_tuples p1 (num_tuples p1 + num_tuples p2);
  set_next_leaf p1 (next_leaf p2)
;;

let delete_tuple_at p pos =
  assert (num_tuples p > 0);
  let t = tuple p pos in
  shift_tuples_left p (pos + 1);
  set_num_tuples p (num_tuples p - 1);
  t
;;

let delete_lowest_tuple p = delete_tuple_at p 0
let delete_highest_tuple p = delete_tuple_at p (num_tuples p - 1)

let delete_tuple p t is_root =
  let key = key_of_tuple p t in
  let pos = find_key_pos p key in
  assert (pos < num_tuples p);
  delete_tuple_at p pos |> ignore;
  if (not is_root) && num_tuples p < min_num_tuples p then Underfull else Deleted
;;

let scan_page p = tuples p |> Array.to_seq |> Seq.take (num_tuples p)
