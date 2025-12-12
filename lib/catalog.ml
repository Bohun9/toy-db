type packed_dbfile =
  | PackedDBFile : (module Db_file.DBFILE with type t = 't) * 't -> packed_dbfile

type t =
  { dir : string
  ; buf_pool : Buffer_pool.t
  ; tables : (string, packed_dbfile) Hashtbl.t
  }

let create dir buf_pool = { dir; buf_pool; tables = Hashtbl.create 16 }
let table_path cat tn = cat.dir ^ "/" ^ tn ^ ".tbl"
let catalog_path cat cn = cat.dir ^ "/" ^ cn ^ ".cat"
let remove_if_exists path = if Sys.file_exists path then Sys.remove path

let assert_table_not_exists cat name =
  if Hashtbl.mem cat.tables name then raise (Error.DBError Error.Table_already_exists)
;;

let add_table cat name desc ~clear =
  assert_table_not_exists cat name;
  let file_path = table_path cat name in
  if clear then remove_if_exists file_path;
  let hf = Heap_file.create file_path desc cat.buf_pool in
  Hashtbl.add cat.tables name (PackedDBFile ((module Heap_file), hf))
;;

let get_table cat name = Hashtbl.find cat.tables name

let delete_table cat name =
  let file_path = table_path cat name in
  remove_if_exists file_path;
  Hashtbl.remove cat.tables name
;;

let process_ddl_query cat = function
  | Syntax.CreateTable (name, schema) ->
    let desc = Tuple.trans_table_schema schema in
    add_table cat name desc ~clear:true
  | Syntax.DropTable name -> delete_table cat name
;;

let get_table_names cat = List.of_seq (Hashtbl.to_seq_keys cat.tables)

let get_table_desc cat name =
  let (PackedDBFile (m, f)) = get_table cat name in
  let module M = (val m) in
  M.get_desc f
;;

let sync_to_disk cat = Buffer_pool.flush_all_pages cat.buf_pool
