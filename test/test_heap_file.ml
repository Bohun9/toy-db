open OUnit2
open Toydb

let desc = [ Tuple.FieldMetadata { name = PureFieldName "number"; typ = TInt } ]
let make_tuple n = Tuple.Tuple { desc; values = [ VInt n ]; rid = None }

let cmp_tuple (Tuple.Tuple t1) (Tuple.Tuple t2) =
  t1.desc = t2.desc && t1.values = t2.values
;;

let show_tuple = Tuple.show_tuple

let with_temp_heap_file f =
  let lm = Lock_manager.create () in
  let buf_pool = Buffer_pool.create 1 lm in
  let tid = Transaction.fresh_tid () in
  Buffer_pool.begin_transaction buf_pool tid;
  let temp_file = Filename.temp_file "heap_file_test_" ".db" in
  try
    let hf = Heap_file.create temp_file desc buf_pool in
    f hf tid;
    Sys.remove temp_file
  with
  | e ->
    Sys.remove temp_file;
    raise e
;;

let test_insert _ =
  with_temp_heap_file (fun hf tid ->
    Heap_file.insert_tuple hf (make_tuple 1) tid;
    Heap_file.insert_tuple hf (make_tuple 2) tid;
    let tuples = List.of_seq (Heap_file.scan_file hf tid) in
    assert_equal ~printer:string_of_int 2 (List.length tuples);
    assert_equal ~cmp:cmp_tuple ~printer:show_tuple (make_tuple 1) (List.nth tuples 0);
    assert_equal ~cmp:cmp_tuple ~printer:show_tuple (make_tuple 2) (List.nth tuples 1))
;;

let test_delete _ =
  with_temp_heap_file (fun hf tid ->
    Heap_file.insert_tuple hf (make_tuple 1) tid;
    Heap_file.insert_tuple hf (make_tuple 2) tid;
    let tuples = Heap_file.scan_file hf tid in
    Seq.iter (fun (Tuple.Tuple t) -> Heap_file.delete_tuple hf t.rid tid) tuples;
    let tuples = Heap_file.scan_file hf tid in
    assert_equal ~printer:string_of_int 0 (Seq.length tuples))
;;

let suite = "heap_file" >::: [ "insert" >:: test_insert; "delete" >:: test_delete ]
let () = run_test_tt_main suite
