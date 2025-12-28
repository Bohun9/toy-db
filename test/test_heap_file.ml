open OUnit2
module U = Test_utils

let test_insert _ =
  U.with_temp_heap_file U.counter_schema (fun hf bp ->
    U.with_tid bp (fun tid ->
      Storage.Heap_file.insert_tuple hf (U.make_counter_tuple 0) tid;
      Storage.Heap_file.insert_tuple hf (U.make_counter_tuple 1) tid;
      let tuples = List.of_seq (Storage.Heap_file.scan_file hf tid) in
      U.assert_int_eq 2 (List.length tuples);
      U.assert_tuple_eq (U.make_counter_tuple 0) (List.nth tuples 0);
      U.assert_tuple_eq (U.make_counter_tuple 1) (List.nth tuples 1)))
;;

let test_delete _ =
  U.with_temp_heap_file U.counter_schema (fun hf bp ->
    U.with_tid bp (fun tid ->
      Storage.Heap_file.insert_tuple hf (U.make_counter_tuple 1) tid;
      Storage.Heap_file.insert_tuple hf (U.make_counter_tuple 2) tid;
      let tuples = Storage.Heap_file.scan_file hf tid in
      Seq.iter
        (fun (t : Core.Tuple.t) -> Storage.Heap_file.delete_tuple hf t.rid tid)
        tuples;
      let tuples = Storage.Heap_file.scan_file hf tid in
      U.assert_int_eq 0 (Seq.length tuples)))
;;

let suite = "heap_file" >::: [ "insert" >:: test_insert; "delete" >:: test_delete ]
let () = run_test_tt_main suite
