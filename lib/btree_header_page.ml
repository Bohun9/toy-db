(* The root page number must be accessed via [buffer_pool.t]
   so that updates can be rolled back on transaction abort. *)

type header_data = { mutable root : int }
type t = header_data Generic_page.t

let root (p : t) = p.data.root

let set_root (p : t) v =
  p.data.root <- v;
  Generic_page.set_dirty p
;;

let create root = Generic_page.create 0 { root }

let serialize p =
  let b = Buffer.create Db_file.page_size in
  Buffer.add_int64_le b (root p |> Int64.of_int);
  Buffer.to_bytes b
;;

let deserialize data =
  let c = Cursor.create data in
  let root = Cursor.read_int64_le c |> Int64.to_int in
  create root
;;
