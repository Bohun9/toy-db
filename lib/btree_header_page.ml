(* The root page number must be accessed via [buffer_pool.t]
   so that updates can be rolled back on transaction abort. *)
type t =
  { mutable root : int
  ; mutable dirty : bool
  }

let is_dirty f = f.dirty
let clear_dirty f = f.dirty <- false
let root f = f.root

let set_root f new_root =
  f.root <- new_root;
  f.dirty <- true
;;

let create root = { root; dirty = false }

let print_bytes_hex b =
  Bytes.iter (fun c -> Printf.printf "%02X " (Char.code c)) b;
  print_newline ()
;;

let serialize p =
  let b = Buffer.create Db_file.page_size in
  Buffer.add_int64_le b (Int64.of_int p.root);
  Buffer.to_bytes b
;;

let deserialize data =
  let c = Cursor.create data in
  let root = Cursor.read_int64_le c |> Int64.to_int in
  { root; dirty = false }
;;
