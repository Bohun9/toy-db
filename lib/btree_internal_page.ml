let node_type_id = '0'

(*
   p_0 | k_0 | p_1 | k_1 | ... | p_{m-1} | k_{m-1} | p_m
*)

type t =
  { page_no : int
  ; max_num_keys : int
  ; mutable parent : int option
  ; mutable keys : Tuple.value list
  ; mutable children : int list
  ; mutable dirty : bool
  }

let min_num_keys p = p.max_num_keys / 2
let num_keys p = List.length p.keys
let num_children p = List.length p.children
let page_no p = p.page_no
let parent_opt p = p.parent
let is_dirty f = f.dirty
let clear_dirty f = f.dirty <- false
let set_dirty f = f.dirty <- true

let parent p =
  match p.parent with
  | Some parent -> parent
  | None -> failwith "internal error"
;;

let set_parent p parent =
  p.parent <- parent;
  set_dirty p
;;

let create page_no parent keys children =
  { page_no; max_num_keys = 2; parent; keys; children; dirty = true }
;;

let find_child p k =
  match List.find_index (Tuple.value_lt k) p.keys with
  | Some i -> List.nth p.children i
  | None -> List.nth p.children (List.length p.keys)
;;

let parent_to_int p = Option.value p.parent ~default:0

let parent_of_int p =
  match p with
  | 0 -> None
  | p -> Some p
;;

let serialize p =
  assert (num_children p = num_keys p + 1);
  let b = Buffer.create Db_file.page_size in
  Buffer.add_char b node_type_id;
  Buffer.add_int64_le b (parent_to_int p |> Int64.of_int);
  Buffer.add_int16_le b (num_keys p);
  List.iter (Db_file.serialize_value b) p.keys;
  List.iter (fun child -> Buffer.add_int64_le b (Int64.of_int child)) p.children;
  Buffer.to_bytes b
;;

let deserialize page_no key_type data =
  let c = Cursor.create data in
  let node_type = Cursor.read_char c in
  assert (node_type = node_type_id);
  let parent = Cursor.read_int64_le c |> Int64.to_int in
  let num_keys = Cursor.read_int16_le c in
  let keys = List.init num_keys (fun _ -> Db_file.deserialize_value c key_type) in
  let children =
    List.init (num_keys + 1) (fun _ -> Cursor.read_int64_le c |> Int64.to_int)
  in
  { page_no
  ; max_num_keys = 2
  ; parent = parent_of_int parent
  ; keys
  ; children
  ; dirty = false
  }
;;

type insert_result =
  | Inserted
  | Split of
      { sep_key : Tuple.value
      ; keys : Tuple.value list
      ; children : int list
      }

let uncons xs = List.hd xs, List.tl xs
let split_at i xs = List.take i xs, List.drop i xs
let insert_at i x xs = List.take i xs @ (x :: List.drop i xs)
let insert_into_keys p k i = p.keys <- insert_at i k p.keys
let insert_into_children p child i = p.children <- insert_at i child p.children
let delete_at i xs = List.take i xs @ List.drop (i + 1) xs

let insert_entry p k child =
  let index =
    match List.find_index (Tuple.value_lt k) p.keys with
    | Some i -> i
    | None -> List.length p.keys
  in
  insert_into_keys p k index;
  insert_into_children p child (index + 1);
  p.dirty <- true;
  if num_keys p <= p.max_num_keys
  then Inserted
  else (
    let size1 = (num_keys p - 1) / 2 in
    let keys1, keys2 = split_at size1 p.keys in
    let sep_key, keys2 = uncons keys2 in
    let children1, children2 = split_at (size1 + 1) p.children in
    p.keys <- keys1;
    p.children <- children1;
    Split { sep_key; keys = keys2; children = children2 })
;;

(* let insert_entry_no_split p k child = *)
(*   match insert_entry p k child with *)
(*   | Inserted -> () *)
(*   | Split _ -> failwith "internal error" *)
(* ;; *)

let insert_entry_at_beggining p child k =
  p.keys <- k :: p.keys;
  p.children <- child :: p.children;
  set_dirty p
;;

let insert_entry_at_end p k child =
  p.keys <- p.keys @ [ k ];
  p.children <- p.children @ [ child ];
  set_dirty p
;;

type sibling =
  { node : int
  ; sep_key : Tuple.value
  ; left : bool
  }

let get_sibling p child =
  match List.find_index (( = ) child) p.children with
  | Some index ->
    if index > 0
    then
      { node = List.nth p.children (index - 1)
      ; sep_key = List.nth p.keys (index - 1)
      ; left = true
      }
    else
      { node = List.nth p.children (index + 1)
      ; sep_key = List.nth p.keys index
      ; left = false
      }
  | None -> failwith "internal error"
;;

let replace_key p k1 k2 =
  assert (List.mem k1 p.keys);
  p.keys <- List.map (fun k -> if k = k1 then k2 else k) p.keys;
  set_dirty p
;;

type delete_result =
  | Deleted
  | EmptyRoot of { new_root : int }
  | Underfull

let can_coalesce p1 p2 =
  assert (p1.max_num_keys = p2.max_num_keys);
  num_keys p1 + 1 + num_keys p2 <= p1.max_num_keys
;;

let coalesce p1 k p2 =
  p1.keys <- p1.keys @ (k :: p2.keys);
  p1.children <- p1.children @ p2.children;
  set_dirty p1;
  p2.children, page_no p1
;;

let delete_lowest_entry p =
  let lowest_child = List.hd p.children in
  let lowest_key = List.hd p.keys in
  p.children <- List.tl p.children;
  p.keys <- List.tl p.keys;
  set_dirty p;
  lowest_child, lowest_key
;;

let delete_highest_entry p =
  let highest_child = List.nth p.children (num_children p - 1) in
  let highest_key = List.nth p.keys (num_keys p - 1) in
  p.children <- List.take (num_children p - 1) p.children;
  p.keys <- List.take (num_keys p - 1) p.keys;
  set_dirty p;
  highest_key, highest_child
;;

let delete_entry p k child is_root =
  match List.find_index (( = ) k) p.keys with
  | Some index ->
    p.keys <- delete_at index p.keys;
    p.children <- delete_at (index + 1) p.children;
    set_dirty p;
    if is_root
    then if num_keys p = 0 then EmptyRoot { new_root = List.hd p.children } else Deleted
    else if num_keys p < min_num_keys p
    then Underfull
    else Deleted
  | None -> failwith "internal error"
;;
