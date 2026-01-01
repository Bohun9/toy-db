module C = Core
module M = Metadata

type t =
  { file : string
  ; schema : M.Table_schema.t
  ; buf_pool : Buffer_pool.t
  }

let file_path f = f.file
let schema f = f.schema
let page_key f page_no = { Page_key.file = f.file; page_no }

let load_header_page f =
  Storage_layout.Page_io.read_page f.file 0
  |> Heap_header_page.deserialize 0
  |> fun p -> Db_page.DB_HeapPage (Heap_page.HeaderPage p)
;;

let load_data_page f page_no =
  Storage_layout.Page_io.read_page f.file page_no
  |> Heap_data_page.deserialize page_no f.schema
  |> fun p -> Db_page.DB_HeapPage (Heap_page.DataPage p)
;;

let flush_heap_page f p =
  Heap_page.serialize p |> Storage_layout.Page_io.write_page f.file (Heap_page.page_no p)
;;

let flush_header_page f p = flush_heap_page f (Heap_page.HeaderPage p)
let flush_data_page f p = flush_heap_page f (Heap_page.DataPage p)

let flush_page f = function
  | Db_page.DB_HeapPage p -> flush_heap_page f p
  | _ -> failwith "internal error"
;;

let initialize f =
  let header = Heap_header_page.create 0 0 in
  flush_header_page f header
;;

let create file schema buf_pool =
  let f = { file; schema; buf_pool } in
  let num_pages = Storage_layout.Page_io.num_pages file in
  if num_pages = 0 then initialize f;
  f
;;

let get_page f page_no tid perm load flush =
  let db_page =
    Buffer_pool.get_page f.buf_pool (page_key f page_no) tid perm load flush
  in
  match db_page with
  | DB_HeapPage p -> p
  | _ -> failwith "internal error"
;;

let get_header_page f tid perm =
  let heap_page = get_page f 0 tid perm (fun () -> load_header_page f) (flush_page f) in
  match heap_page with
  | Heap_page.HeaderPage p -> p
  | _ -> failwith "internal error"
;;

let get_data_page f page_no tid perm =
  let heap_page =
    get_page f page_no tid perm (fun () -> load_data_page f page_no) (flush_page f)
  in
  match heap_page with
  | Heap_page.DataPage p -> p
  | _ -> failwith "internal error"
;;

let get_num_data_pages f tid =
  let h = get_header_page f tid Lock_manager.ReadPerm in
  Heap_header_page.num_data_pages h
;;

let set_num_data_pages f v tid =
  let h = get_header_page f tid Lock_manager.WritePerm in
  Heap_header_page.set_num_data_pages h v
;;

let data_page_numbers n = List.init n (fun n -> n + 1)

let insert_tuple f t tid =
  let num_data_pages = get_num_data_pages f tid in
  let target_page_no =
    data_page_numbers num_data_pages
    |> List.find_opt (fun page_no ->
      let p = get_data_page f page_no tid Lock_manager.ReadPerm in
      Heap_data_page.has_empty_slot p)
  in
  match target_page_no with
  | Some page_no ->
    let p = get_data_page f page_no tid Lock_manager.WritePerm in
    Heap_data_page.insert_tuple p t
  | None ->
    set_num_data_pages f (num_data_pages + 1) tid;
    let new_page = Heap_data_page.create (num_data_pages + 1) f.schema in
    Heap_data_page.insert_tuple new_page t;
    flush_data_page f new_page
;;

let delete_tuple f rid tid =
  let (C.Record_id.{ page_no; _ } as rid) = Option.get rid in
  let p = get_data_page f page_no tid Lock_manager.WritePerm in
  Heap_data_page.delete_tuple p rid
;;

let scan_file f tid =
  data_page_numbers (get_num_data_pages f tid)
  |> List.to_seq
  |> Seq.flat_map (fun page_no ->
    let p = get_data_page f page_no tid Lock_manager.ReadPerm in
    Heap_data_page.scan_page p)
;;
