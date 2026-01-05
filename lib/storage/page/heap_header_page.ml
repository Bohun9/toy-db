type header_data = { mutable num_data_pages : int }
type t = header_data Generic_page.t

let num_data_pages (p : t) = p.data.num_data_pages

let set_num_data_pages (p : t) v =
  p.data.num_data_pages <- v;
  Generic_page.set_dirty p
;;

let create page_no num_data_pages = Generic_page.create page_no { num_data_pages }

let serialize p =
  let b = Buffer.create Layout.page_size in
  Buffer.add_int64_le b (num_data_pages p |> Int64.of_int);
  Buffer.to_bytes b
;;

let deserialize page_no data =
  let c = Cursor.create data in
  let num_data_pages = Cursor.read_int64_le c |> Int64.to_int in
  create page_no num_data_pages
;;
