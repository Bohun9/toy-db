(* The root page number must be accessed via [buffer_pool.t]
   so that updates can be rolled back on transaction abort. *)

type header_data = { mutable root : int }
type t = header_data Generic_page.t

let root (p : t) = p.data.root

let set_root (p : t) v =
  p.data.root <- v;
  Generic_page.set_dirty p
;;

let create page_no root = Generic_page.create page_no { root }

let serialize p =
  let b = Buffer.create Storage_layout.Page_io.page_size in
  Buffer.add_int64_le b (root p |> Int64.of_int);
  Buffer.to_bytes b
;;

let deserialize page_no data =
  let c = Cursor.create data in
  let root = Cursor.read_int64_le c |> Int64.to_int in
  create page_no root
;;
