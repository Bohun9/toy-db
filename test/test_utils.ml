open Core
open Metadata
open Toydb

let rec shuffle = function
  | [] -> []
  | [ x ] -> [ x ]
  | xs ->
    let l, r = List.partition (fun _ -> Random.bool ()) xs in
    List.rev_append (shuffle l) (shuffle r)
;;

let counter_schema =
  Table_schema.create [ { name = "counter"; typ = TInt } ] None |> Result.get_ok
;;

let make_counter_tuple n : Tuple.t = { values = [ VInt n ]; rid = None }
let cmp_tuple (t1 : Tuple.t) (t2 : Tuple.t) = t1.values = t2.values
let assert_int_eq = OUnit2.assert_equal ~printer:string_of_int
let assert_tuple_eq = OUnit2.assert_equal ~cmp:cmp_tuple ~printer:Tuple.show

let with_temp_file prefix sufix f =
  let temp_file = Filename.temp_file prefix sufix in
  try
    let r = f temp_file in
    Sys.remove temp_file;
    r
  with
  | e ->
    Sys.remove temp_file;
    raise e
;;

let with_temp_dir prefix sufix f =
  let temp_dir = Filename.temp_dir prefix sufix in
  try
    let r = f temp_dir in
    Sys.rmdir temp_dir;
    r
  with
  | e ->
    Sys.rmdir temp_dir;
    raise e
;;

let create_buf_pool max_num_pages =
  let lm = Storage.Lock_manager.create () in
  let bp = Storage.Buffer_pool.create max_num_pages lm in
  bp
;;

let with_catalog dir f =
  let cat = Catalog.create dir 1000 in
  f cat
;;

let with_temp_heap_file schema f =
  with_temp_file "temp_heap_file_" ".tbl" (fun temp_file ->
    let bp = create_buf_pool 4 in
    let hf = Storage.Heap_file.create temp_file schema bp in
    f hf bp)
;;

let with_temp_btree_file schema key_field f =
  with_temp_file "temp_btree_file_" ".tbl" (fun temp_file ->
    let bp = create_buf_pool 1000 in
    let bt = Storage.Btree_file.create temp_file schema bp key_field in
    f bt bp)
;;

let with_tid bp f =
  let tid = Transaction_id.fresh_tid () in
  Storage.Buffer_pool.begin_transaction bp tid;
  let r = f tid in
  Storage.Buffer_pool.commit_transaction bp tid;
  r
;;
