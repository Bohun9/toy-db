let node_type_id = '0'

(*
   p_0 | k_0 | p_1 | k_1 | ... | p_{m-1} | k_{m-1} | p_m
*)

type internal_data =
  { max_num_keys : int
  ; mutable keys : Value.t list
  ; mutable children : int list
  }

type t = internal_data Generic_page.t Btree_node.t

let max_num_keys (p : t) = p.data.data.max_num_keys
let keys (p : t) = p.data.data.keys
let children (p : t) = p.data.data.children

let set_keys (p : t) v =
  p.data.data.keys <- v;
  Btree_node.set_dirty p
;;

let set_children (p : t) v =
  p.data.data.children <- v;
  Btree_node.set_dirty p
;;

let min_num_keys p = max_num_keys p / 2
let num_keys p = List.length (keys p)
let num_children p = List.length (children p)

let create page_no parent keys children =
  Btree_node.create parent
  @@ Generic_page.create page_no { max_num_keys = 2; keys; children }
;;

let find_child p k =
  match List.find_index (Value.eval_lt k) (keys p) with
  | Some i -> List.nth (children p) i
  | None -> List.nth (children p) (List.length (keys p))
;;

let serialize p =
  assert (num_children p = num_keys p + 1);
  let b = Buffer.create Db_file.page_size in
  Buffer.add_char b node_type_id;
  Buffer.add_int64_le
    b
    (Btree_node.parent_opt p |> Btree_node.encode_parent |> Int64.of_int);
  Buffer.add_int16_le b (num_keys p);
  List.iter (Db_file.serialize_value b) (keys p);
  List.iter (fun child -> Buffer.add_int64_le b (Int64.of_int child)) (children p);
  Buffer.to_bytes b
;;

let deserialize page_no key_type data =
  let c = Cursor.create data in
  let node_type = Cursor.read_char c in
  assert (node_type = node_type_id);
  let parent = Cursor.read_int64_le c |> Int64.to_int |> Btree_node.decode_parent in
  let num_keys = Cursor.read_int16_le c in
  let keys = List.init num_keys (fun _ -> Db_file.deserialize_value c key_type) in
  let children =
    List.init (num_keys + 1) (fun _ -> Cursor.read_int64_le c |> Int64.to_int)
  in
  create page_no parent keys children
;;

type insert_result =
  | Inserted
  | Split of
      { sep_key : Value.t
      ; keys : Value.t list
      ; children : int list
      }

let uncons xs = List.hd xs, List.tl xs
let split_at i xs = List.take i xs, List.drop i xs
let insert_at i x xs = List.take i xs @ (x :: List.drop i xs)
let insert_into_keys p k i = set_keys p @@ insert_at i k (keys p)
let insert_into_children p child i = set_children p @@ insert_at i child (children p)
let delete_at i xs = List.take i xs @ List.drop (i + 1) xs

let insert_entry p k child =
  let index =
    match List.find_index (Value.eval_lt k) (keys p) with
    | Some i -> i
    | None -> List.length (keys p)
  in
  insert_into_keys p k index;
  insert_into_children p child (index + 1);
  if num_keys p <= max_num_keys p
  then Inserted
  else (
    let size1 = (num_keys p - 1) / 2 in
    let keys1, keys2 = split_at size1 (keys p) in
    let sep_key, keys2 = uncons keys2 in
    let children1, children2 = split_at (size1 + 1) (children p) in
    set_keys p keys1;
    set_children p children1;
    Split { sep_key; keys = keys2; children = children2 })
;;

let insert_entry_at_beggining p child k =
  set_keys p @@ (k :: keys p);
  set_children p @@ (child :: children p)
;;

let insert_entry_at_end p k child =
  set_keys p @@ keys p @ [ k ];
  set_children p @@ children p @ [ child ]
;;

type sibling =
  { node : int
  ; sep_key : Value.t
  ; left : bool
  }

let get_sibling p child =
  match List.find_index (( = ) child) (children p) with
  | Some index ->
    if index > 0
    then
      { node = List.nth (children p) (index - 1)
      ; sep_key = List.nth (keys p) (index - 1)
      ; left = true
      }
    else
      { node = List.nth (children p) (index + 1)
      ; sep_key = List.nth (keys p) index
      ; left = false
      }
  | None -> failwith "internal error"
;;

let replace_key p k1 k2 =
  assert (List.mem k1 (keys p));
  set_keys p @@ List.map (fun k -> if k = k1 then k2 else k) (keys p)
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
  set_keys p1 @@ keys p1 @ (k :: keys p2);
  set_children p1 @@ children p1 @ children p2;
  children p2, Btree_node.page_no p1
;;

let delete_lowest_entry p =
  let lowest_child = List.hd (children p) in
  let lowest_key = List.hd (keys p) in
  set_children p @@ List.tl (children p);
  set_keys p @@ List.tl (keys p);
  lowest_child, lowest_key
;;

let delete_highest_entry p =
  let highest_child = List.nth (children p) (num_children p - 1) in
  let highest_key = List.nth (keys p) (num_keys p - 1) in
  set_children p @@ List.take (num_children p - 1) (children p);
  set_keys p @@ List.take (num_keys p - 1) (keys p);
  highest_key, highest_child
;;

let delete_entry p k child is_root =
  match List.find_index (( = ) k) (keys p) with
  | Some index ->
    set_keys p @@ delete_at index (keys p);
    set_children p @@ delete_at (index + 1) (children p);
    if is_root
    then if num_keys p = 0 then EmptyRoot { new_root = List.hd (children p) } else Deleted
    else if num_keys p < min_num_keys p
    then Underfull
    else Deleted
  | None -> failwith "internal error"
;;
