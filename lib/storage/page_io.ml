module C = Core
module M = Metadata

let page_size = Page.Layout.page_size

let read_page file page_no =
  let offset = page_no * page_size in
  In_channel.with_open_bin file (fun ic ->
    In_channel.seek ic (Int64.of_int offset);
    let data = Bytes.create page_size in
    (match In_channel.really_input ic data 0 (Bytes.length data) with
     | Some _ -> ()
     | None -> failwith "internal error - load_raw_page");
    data)
;;

let write_page file page_no data =
  let len = Bytes.length data in
  assert (len <= page_size);
  let offset = page_no * page_size in
  Out_channel.with_open_gen [ Out_channel.Open_wronly ] 0o666 file (fun oc ->
    Out_channel.seek oc (Int64.of_int offset);
    Out_channel.output_bytes oc data;
    if len < page_size
    then Out_channel.output_bytes oc (Bytes.make (page_size - len) '\x00'))
;;

let num_pages file =
  let len =
    In_channel.with_open_gen [ In_channel.Open_creat ] 0o666 file (fun ic ->
      Int64.to_int (In_channel.length ic))
  in
  assert (len mod page_size = 0);
  let num_pages = len / page_size in
  num_pages
;;
