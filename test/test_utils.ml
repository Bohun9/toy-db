open Toydb

let counter_desc = [ Tuple.FieldMetadata { name = PureFieldName "counter"; typ = TInt } ]

let make_counter_tuple n =
  Tuple.Tuple { desc = counter_desc; values = [ VInt n ]; rid = None }
;;

let cmp_tuple (Tuple.Tuple t1) (Tuple.Tuple t2) =
  t1.desc = t2.desc && t1.values = t2.values
;;

let assert_int_eq = OUnit2.assert_equal ~printer:string_of_int
let assert_tuple_eq = OUnit2.assert_equal ~cmp:cmp_tuple ~printer:Tuple.show_tuple

let with_temp_heap_file desc f =
  let lm = Lock_manager.create () in
  let bp = Buffer_pool.create 1 lm in
  let temp_file = Filename.temp_file "transaction_test_" ".db" in
  let hf = Heap_file.create temp_file desc bp in
  try
    f hf bp;
    Sys.remove temp_file
  with
  | e ->
    Sys.remove temp_file;
    raise e
;;

let with_tid bp f =
  let tid = Transaction_id.fresh_tid () in
  Buffer_pool.begin_transaction bp tid;
  let r = f tid in
  Buffer_pool.commit_transaction bp tid;
  r
;;
