open Toydb
module C = Core
module M = Metadata
module S = Storage

let rec shuffle = function
  | [] -> []
  | [ x ] -> [ x ]
  | xs ->
    let l, r = List.partition (fun _ -> Random.bool ()) xs in
    List.rev_append (shuffle l) (shuffle r)
;;

let counter_schema = M.Table_schema.create [ C.{ name = "counter"; typ = Type.Int } ] None
let make_counter_tuple n = C.Tuple.create [ C.Value.Int n ]
let cmp_tuple t1 t2 = C.Tuple.attributes t1 = C.Tuple.attributes t2
let assert_int_eq = OUnit2.assert_equal ~printer:string_of_int
let assert_tuple_eq = OUnit2.assert_equal ~cmp:cmp_tuple ~printer:C.Tuple.show

let with_temp_file prefix sufix f =
  let temp_file = Filename.temp_file prefix sufix in
  Fun.protect ~finally:(fun () -> Sys.remove temp_file) (fun () -> f temp_file)
;;

let with_temp_dir f =
  let temp_dir = Filename.temp_dir "temp_db_catalog_" "" in
  Fun.protect ~finally:(fun () -> Sys.rmdir temp_dir) (fun () -> f temp_dir)
;;

let create_buf_pool max_num_pages =
  let lm = S.Lock_manager.create () in
  let bp = S.Buffer_pool.create max_num_pages lm in
  bp
;;

let with_catalog dir f =
  let cat = Catalog.create dir 1000 in
  f cat
;;

let with_temp_catalog f =
  with_temp_dir (fun dir ->
    with_catalog dir (fun cat ->
      Fun.protect ~finally:(fun () -> Catalog.delete_db_files cat) (fun () -> f cat)))
;;

let with_temp_heap_file schema f =
  with_temp_file "temp_heap_file_" ".tbl" (fun temp_file ->
    let bp = create_buf_pool 4 in
    let hf = S.Heap_file.create temp_file schema bp in
    f hf bp)
;;

let with_temp_btree_file schema key_attribute f =
  with_temp_file "temp_btree_file_" ".tbl" (fun temp_file ->
    let bp = create_buf_pool 1000 in
    let bt = S.Btree_file.create temp_file schema bp key_attribute in
    f bt bp)
;;

let with_tid bp f =
  let tid = C.Transaction_id.fresh_tid () in
  S.Buffer_pool.begin_transaction bp tid;
  let r = f tid in
  S.Buffer_pool.commit_transaction bp tid;
  r
;;

let execute_ddl sql cat tid =
  match Catalog.execute_sql sql cat tid with
  | Catalog.Rows _ -> OUnit2.assert_failure "expected no result but got rows"
  | Catalog.NoResult -> ()
;;

let execute_dml sql cat tid =
  match Catalog.execute_sql sql cat tid with
  | Catalog.Rows _ -> ()
  | Catalog.NoResult -> OUnit2.assert_failure "expected rows but got no result"
;;

let execute_stmt sql cat tid =
  match Catalog.execute_sql sql cat tid with
  | Catalog.Rows rows -> rows
  | Catalog.NoResult -> OUnit2.assert_failure "expected rows but got no result"
;;
