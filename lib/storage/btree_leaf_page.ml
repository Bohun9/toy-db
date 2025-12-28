open Core

let node_type_id = '1'

type leaf_data =
  { max_num_tuples : int
  ; key_field : int
  ; mutable tuples : Tuple.t list
  ; mutable next_leaf_page : int option
  }

type t = leaf_data Generic_page.t Btree_node.t

let max_num_tuples (p : t) = p.data.data.max_num_tuples
let key_field (p : t) = p.data.data.key_field
let tuples (p : t) = p.data.data.tuples
let next_leaf_page (p : t) = p.data.data.next_leaf_page

let set_tuples (p : t) v =
  p.data.data.tuples <- v;
  Btree_node.set_dirty p
;;

let set_next_leaf_page (p : t) v =
  p.data.data.next_leaf_page <- v;
  Btree_node.set_dirty p
;;

let min_num_tuples p = (max_num_tuples p + 1) / 2
let num_tuples p = List.length (tuples p)
let get_key p t = Tuple.field t (key_field p)
let lowest_key p = get_key p (List.hd @@ tuples p)

let create page_no key_field parent tuples next_leaf_page =
  Btree_node.create parent
  @@ Generic_page.create page_no { max_num_tuples = 2; key_field; tuples; next_leaf_page }
;;

let encode_next_leaf_page p = Option.value (next_leaf_page p) ~default:0

let decode_next_leaf_page p =
  match p with
  | 0 -> None
  | p -> Some p
;;

let serialize p =
  let b = Buffer.create Storage_layout.page_size in
  Buffer.add_char b node_type_id;
  Buffer.add_int64_le
    b
    (Btree_node.parent_opt p |> Btree_node.encode_parent |> Int64.of_int);
  Buffer.add_int64_le b (encode_next_leaf_page p |> Int64.of_int);
  Buffer.add_int16_le b (num_tuples p);
  List.iter (Storage_layout.serialize_tuple b) (tuples p);
  Buffer.to_bytes b
;;

let deserialize page_no schema key_field data =
  let c = Cursor.create data in
  let node_type = Cursor.read_char c in
  assert (node_type = node_type_id);
  let parent = Cursor.read_int64_le c |> Int64.to_int |> Btree_node.decode_parent in
  let next_leaf_page = Cursor.read_int64_le c |> Int64.to_int |> decode_next_leaf_page in
  let num_tuples = Cursor.read_int16_le c in
  let tuples = List.init num_tuples (Storage_layout.deserialize_tuple c schema page_no) in
  create page_no key_field parent tuples next_leaf_page
;;

type insert_result =
  | Inserted
  | Split of Tuple.t list

let split_at i xs = List.take i xs, List.drop i xs

let insert_tuple (p : t) t =
  let k = get_key p t in
  let new_tuples =
    match List.find_index (fun t -> Value.eval_lt k (get_key p t)) (tuples p) with
    | Some i ->
      let l, r = split_at i (tuples p) in
      l @ (t :: r)
    | None -> tuples p @ [ t ]
  in
  if num_tuples p <= max_num_tuples p
  then (
    set_tuples p new_tuples;
    Inserted)
  else (
    let half = List.length new_tuples / 2 in
    let l, r = split_at half new_tuples in
    set_tuples p l;
    Split r)
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
  List.iter (insert_tuple_no_split p1) (tuples p2);
  set_next_leaf_page p1 (next_leaf_page p2)
;;

let delete_lowest_tuple p =
  let lowest_tuple = List.hd (tuples p) in
  set_tuples p (List.tl (tuples p));
  lowest_tuple
;;

let delete_highest_tuple p =
  let highest_tuple = List.nth (tuples p) (List.length (tuples p) - 1) in
  set_tuples p @@ List.take (List.length (tuples p) - 1) (tuples p);
  highest_tuple
;;

let delete_tuple p t is_root =
  let k = get_key p t in
  let new_tuples = List.filter (fun t -> k <> get_key p t) (tuples p) in
  assert (List.length new_tuples + 1 = List.length (tuples p));
  set_tuples p new_tuples;
  if is_root || min_num_tuples p <= num_tuples p then Deleted else Underfull
;;

let scan_page p = List.to_seq (tuples p)
