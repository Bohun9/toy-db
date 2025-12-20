type page = Heap_page.t

type t =
  { file : string
  ; desc : Tuple.tuple_descriptor
  ; buf_pool : Buffer_pool.t
  ; num_pages : int Atomic.t
  }

let desc hf = hf.desc
let num_pages hf = Atomic.get hf.num_pages
let incr_num_pages hf = Atomic.incr hf.num_pages
let page_key hf page_no = Page_key.PageKey { file = hf.file; page_no }

let load_heap_page hf page_no =
  let offset = page_no * Heap_page.page_size in
  In_channel.with_open_bin hf.file (fun ic ->
    In_channel.seek ic (Int64.of_int offset);
    let data = Bytes.create Heap_page.page_size in
    (match In_channel.really_input ic data 0 (Bytes.length data) with
     | Some _ -> ()
     | None -> failwith "internal error - load_heap_page");
    Heap_page.deserialize page_no hf.desc data)
;;

let load_page hf page_no = Db_page.DB_HeapPage (load_heap_page hf page_no)

let flush_heap_page hf (hp : Heap_page.t) =
  let offset = hp.page_no * Heap_page.page_size in
  Out_channel.with_open_bin hf.file (fun oc ->
    Out_channel.seek oc (Int64.of_int offset);
    Out_channel.output_bytes oc (Heap_page.serialize hp));
  Heap_page.clear_dirty hp
;;

let flush_page hf dbp =
  match dbp with
  | Db_page.DB_HeapPage hp -> flush_heap_page hf hp
;;

(* Handle phantom-read case where the writing transaction adds a new page.
   This ensures there are no data races when the file has no pages, as the
   later transactions will always block on the first page. *)
let ensure_at_least_one_page hf =
  if num_pages hf = 0
  then (
    let hp = Heap_page.create 0 hf.desc in
    flush_heap_page hf hp)
;;

let create file desc buf_pool =
  let len =
    In_channel.with_open_gen [ In_channel.Open_creat ] 0o666 file (fun ic ->
      Int64.to_int (In_channel.length ic))
  in
  assert (len mod Heap_page.page_size = 0);
  let num_pages = len / Heap_page.page_size in
  let hf = { file; desc; buf_pool; num_pages = Atomic.make num_pages } in
  ensure_at_least_one_page hf;
  hf
;;

let get_page hf page_no tid perm =
  let page =
    Buffer_pool.get_page
      hf.buf_pool
      (page_key hf page_no)
      tid
      perm
      (fun () -> load_page hf page_no)
      (flush_page hf)
  in
  match page with
  | DB_HeapPage hp -> hp
;;

let check_tuple_type hf (t : Tuple.t) =
  if not (Tuple.match_desc hf.desc t.desc) then raise Error.type_mismatch
;;

(* The number of pages may increase during iteration in
   [insert_tuple] and [scan_file], so this function is required. *)
let seq_dynamic_init size_fn f =
  let rec iter i () = if i = size_fn () then Seq.Nil else Seq.Cons (f i, iter (i + 1)) in
  iter 0
;;

let insert_tuple hf (t : Tuple.t) tid =
  check_tuple_type hf t;
  let t = Tuple.set_tuple_desc t hf.desc in
  let pages = seq_dynamic_init (fun () -> num_pages hf) Fun.id in
  match
    Seq.find
      (fun page_no ->
        Heap_page.insert_tuple (get_page hf page_no tid Lock_manager.WritePerm) t)
      pages
  with
  | Some _ -> ()
  | None ->
    let new_page = Heap_page.create (num_pages hf) hf.desc in
    incr_num_pages hf;
    assert (Heap_page.insert_tuple new_page t);
    flush_heap_page hf new_page
;;

let delete_tuple hf rid tid =
  match rid with
  | Some (Tuple.RecordID { page_no; _ } as rid) ->
    let page = get_page hf page_no tid Lock_manager.WritePerm in
    Heap_page.delete_tuple page rid
  | None -> failwith "internal error - delete_tuple"
;;

let scan_file hf tid =
  seq_dynamic_init (fun () -> num_pages hf) Fun.id
  |> Seq.flat_map (fun page_no ->
    Heap_page.scan_page (get_page hf page_no tid Lock_manager.ReadPerm))
;;
