module C = Core

let internal_id = 'I'

(*
   p_0 | k_0 | p_1 | k_1 | ... | p_{m-1} | k_{m-1} | p_m
*)

type internal_data =
  { keys : C.Value.t Array.t
  ; children : int Array.t
  ; mutable num_keys : int
  }

type t = internal_data Generic_page.t

let keys (p : t) = p.data.keys
let children (p : t) = p.data.children
let num_keys (p : t) = p.data.num_keys

let set_num_keys (p : t) v =
  p.data.num_keys <- v;
  Generic_page.set_dirty p
;;

let num_children p = num_keys p + 1
let max_num_keys p = Array.length (keys p) - 1
let min_num_keys p = max_num_keys p / 2
let key p i = (keys p).(i)
let child p i = (children p).(i)
let get_children p = Array.to_list (children p) |> List.take (num_keys p + 1)

type create_data =
  | RootData of
      { child1 : int
      ; key : C.Value.t
      ; child2 : int
      }
  | InternalData of internal_data

let max_num_keys_for_key key_type =
  (Layout.page_size - (1 + 8 + 2) - 8) / (Layout.value_storage_size key_type + 8)
;;

let create_storage key_type =
  let max_num_keys = max_num_keys_for_key key_type in
  let keys = Array.make (max_num_keys + 1) (C.Value.minus_infty key_type) in
  let children = Array.make (max_num_keys + 2) 0 in
  keys, children
;;

let create page_no key_type create_data =
  let data =
    match create_data with
    | RootData { child1; key; child2 } ->
      let keys, children = create_storage key_type in
      keys.(0) <- key;
      children.(0) <- child1;
      children.(1) <- child2;
      { keys; children; num_keys = 1 }
    | InternalData data -> data
  in
  Generic_page.create page_no data
;;

(* [predicate] should be monotonic. *)
let find_first_key p predicate =
  let rec loop i = if i = num_keys p || predicate (key p i) then i else loop (i + 1) in
  loop 0
;;

let find_key_pos p k = find_first_key p (C.Value.eval_le k)
let find_child p k = child p (find_first_key p (C.Value.eval_lt k))

let serialize p =
  let b = Buffer.create Layout.page_size in
  Buffer.add_char b internal_id;
  Buffer.add_int16_le b (num_keys p);
  for i = 0 to num_keys p - 1 do
    Layout.serialize_value b (key p i)
  done;
  for i = 0 to num_children p - 1 do
    Buffer.add_int64_le b (child p i |> Int64.of_int)
  done;
  Buffer.to_bytes b
;;

let deserialize page_no key_type data =
  let c = Cursor.create data in
  assert (Cursor.read_char c = internal_id);
  let num_keys = Cursor.read_int16_le c in
  let keys, children = create_storage key_type in
  for i = 0 to num_keys - 1 do
    keys.(i) <- Layout.deserialize_value c key_type
  done;
  for i = 0 to num_keys do
    children.(i) <- Cursor.read_int64_le c |> Int64.to_int
  done;
  create page_no key_type (InternalData { keys; children; num_keys })
;;

type insert_result =
  | Inserted
  | Split of
      { sep_key : C.Value.t
      ; internal_data : internal_data
      }

let shift_keys_right p from =
  let len = num_keys p - from in
  Array.blit (keys p) from (keys p) (from + 1) len
;;

let shift_children_right p from =
  let len = num_children p - from in
  Array.blit (children p) from (children p) (from + 1) len
;;

let shift_keys_left p from =
  let len = num_keys p - from in
  Array.blit (keys p) from (keys p) (from - 1) len
;;

let shift_children_left p from =
  let len = num_children p - from in
  Array.blit (children p) from (children p) (from - 1) len
;;

let insert_entry_at p k child pos right_child =
  let child_pos = pos + if right_child then 1 else 0 in
  shift_keys_right p pos;
  shift_children_right p child_pos;
  (keys p).(pos) <- k;
  (children p).(child_pos) <- child;
  set_num_keys p (num_keys p + 1);
  if num_keys p <= max_num_keys p
  then Inserted
  else (
    let size1 = (num_keys p - 1) / 2 in
    let size2 = num_keys p - size1 - 1 in
    let keys1 = keys p in
    let keys2 = Array.copy keys1 in
    let sep_key = keys1.(size1) in
    Array.blit keys1 (size1 + 1) keys2 0 size2;
    let children1 = children p in
    let children2 = Array.copy children1 in
    Array.blit children1 (size1 + 1) children2 0 (size2 + 1);
    set_num_keys p size1;
    Split
      { sep_key
      ; internal_data = { keys = keys2; children = children2; num_keys = size2 }
      })
;;

let insert_entry_at_no_split p k child pos right_child =
  match insert_entry_at p k child pos right_child with
  | Inserted -> ()
  | Split _ -> failwith "internal error"
;;

let insert_entry p k child =
  let pos = find_key_pos p k in
  insert_entry_at p k child pos true
;;

let insert_entry_at_beginning p child k = insert_entry_at_no_split p k child 0 false
let insert_entry_at_end p k child = insert_entry_at_no_split p k child (num_keys p) true

type sibling =
  { node : int
  ; sep_key : C.Value.t
  ; left : bool
  }

let find_child_pos p child = Array.find_index (( = ) child) (children p) |> Option.get

let get_sibling p child_ =
  let pos = find_child_pos p child_ in
  if pos > 0
  then { node = child p (pos - 1); sep_key = key p (pos - 1); left = true }
  else { node = child p (pos + 1); sep_key = key p pos; left = false }
;;

let replace_key p k1 k2 =
  let pos = find_key_pos p k1 in
  assert (key p pos = k1);
  (keys p).(pos) <- k2;
  Generic_page.set_dirty p
;;

type delete_result =
  | Deleted
  | EmptyRoot of { new_root : int }
  | Underfull

let can_coalesce p1 p2 =
  assert (max_num_keys p1 = max_num_keys p2);
  num_keys p1 + 1 + num_keys p2 <= max_num_keys p1
;;

let coalesce p1 k p2 =
  assert (can_coalesce p1 p2);
  (keys p1).(num_keys p1) <- k;
  Array.blit (keys p2) 0 (keys p1) (num_keys p1 + 1) (num_keys p2);
  Array.blit (children p2) 0 (children p1) (num_children p1) (num_children p2);
  set_num_keys p1 (num_keys p1 + 1 + num_keys p2);
  get_children p2
;;

let delete_entry_at p pos right_child =
  let child_pos = pos + if right_child then 1 else 0 in
  let k = key p pos in
  let child = child p child_pos in
  shift_keys_left p (pos + 1);
  shift_children_left p (child_pos + 1);
  set_num_keys p (num_keys p - 1);
  k, child
;;

let delete_lowest_entry p = delete_entry_at p 0 false |> Pair.swap
let delete_highest_entry p = delete_entry_at p (num_keys p - 1) true

let delete_entry p k child_ is_root =
  let pos = find_key_pos p k in
  assert (delete_entry_at p pos true |> snd = child_);
  if is_root
  then if num_keys p = 0 then EmptyRoot { new_root = child p 0 } else Deleted
  else if num_keys p < min_num_keys p
  then Underfull
  else Deleted
;;

let safe p = function
  | Btree_op.Read -> true
  | Btree_op.Insert -> num_keys p < max_num_keys p
  | Btree_op.Delete -> num_keys p > min_num_keys p
;;
