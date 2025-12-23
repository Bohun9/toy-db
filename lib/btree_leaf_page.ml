let node_type_id = '1'

type t =
  { page_no : int
  ; max_num_tuples : int
  ; key_field : int
  ; mutable parent : int option
  ; mutable tuples : Tuple.t list
  ; mutable next_leaf_page : int option
  ; mutable dirty : bool
  }

let min_num_tuples p = (p.max_num_tuples + 1) / 2
let max_num_tuples p = p.max_num_tuples
let is_dirty f = f.dirty
let clear_dirty f = f.dirty <- false
let set_dirty f = f.dirty <- true
let page_no p = p.page_no
let num_tuples p = List.length p.tuples
let parent_opt p = p.parent

let parent p =
  match p.parent with
  | Some parent -> parent
  | None -> failwith "internal error"
;;

let set_parent p parent =
  p.parent <- parent;
  set_dirty p
;;

let next_leaf_page p = p.next_leaf_page

let set_next_leaf_page p v =
  p.next_leaf_page <- v;
  set_dirty p
;;

let get_key p t = Tuple.field t p.key_field
let lowest_key p = get_key p (List.hd p.tuples)

let create page_no key_field parent tuples next_leaf_page =
  { page_no; max_num_tuples = 2; key_field; parent; tuples; next_leaf_page; dirty = true }
;;

let parent_to_int p = Option.value p.parent ~default:0
let next_leaf_page_to_int p = Option.value p.next_leaf_page ~default:0

let parent_of_int p =
  match p with
  | 0 -> None
  | p -> Some p
;;

let next_leaf_page_of_int p =
  match p with
  | 0 -> None
  | p -> Some p
;;

let serialize p =
  let b = Buffer.create Db_file.page_size in
  Buffer.add_char b node_type_id;
  Buffer.add_int64_le b (parent_to_int p |> Int64.of_int);
  Buffer.add_int64_le b (next_leaf_page_to_int p |> Int64.of_int);
  Buffer.add_int16_le b (num_tuples p);
  List.iter (Db_file.serialize_tuple b) p.tuples;
  Buffer.to_bytes b
;;

let deserialize page_no desc key_field data =
  let c = Cursor.create data in
  let node_type = Cursor.read_char c in
  assert (node_type = node_type_id);
  let parent = Cursor.read_int64_le c |> Int64.to_int in
  let next_leaf_page = Cursor.read_int64_le c |> Int64.to_int in
  let num_tuples = Cursor.read_int16_le c in
  let tuples = List.init num_tuples (Db_file.deserialize_tuple c desc page_no) in
  { page_no
  ; max_num_tuples = 2
  ; key_field
  ; parent = parent_of_int parent
  ; tuples
  ; next_leaf_page = next_leaf_page_of_int next_leaf_page
  ; dirty = false
  }
;;

type insert_result =
  | Inserted
  | Split of Tuple.t list

let split_at i xs = List.take i xs, List.drop i xs

let insert_tuple p t =
  let k = get_key p t in
  let new_tuples =
    match List.find_index (fun t -> Tuple.value_lt k (get_key p t)) p.tuples with
    | Some i ->
      let l, r = split_at i p.tuples in
      l @ (t :: r)
    | None -> p.tuples @ [ t ]
  in
  set_dirty p;
  if num_tuples p <= p.max_num_tuples
  then (
    p.tuples <- new_tuples;
    Inserted)
  else (
    let half = List.length new_tuples / 2 in
    let l, r = split_at half new_tuples in
    p.tuples <- l;
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
  assert (p1.max_num_tuples = p2.max_num_tuples);
  num_tuples p1 + num_tuples p2 <= p1.max_num_tuples
;;

let coalesce p1 p2 =
  List.iter (insert_tuple_no_split p1) p2.tuples;
  set_next_leaf_page p1 (next_leaf_page p2)
;;

let delete_lowest_tuple p =
  let lowest_tuple = List.hd p.tuples in
  p.tuples <- List.tl p.tuples;
  set_dirty p;
  lowest_tuple
;;

let delete_highest_tuple p =
  let highest_tuple = List.nth p.tuples (List.length p.tuples - 1) in
  p.tuples <- List.take (List.length p.tuples - 1) p.tuples;
  set_dirty p;
  highest_tuple
;;

let delete_tuple p t is_root =
  let k = get_key p t in
  let new_tuples = List.filter (fun t -> k <> get_key p t) p.tuples in
  assert (List.length new_tuples + 1 = List.length p.tuples);
  p.tuples <- new_tuples;
  set_dirty p;
  if is_root || min_num_tuples p <= num_tuples p then Deleted else Underfull
;;

let scan_page p = List.to_seq p.tuples
