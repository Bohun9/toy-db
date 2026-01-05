open OUnit2
module T = Domainslib.Task
module U = Test_utils
module C = Core

let with_heap_file f =
  U.with_temp_heap_file U.counter_schema (fun hf bp -> U.with_tid bp (f hf))
;;

let test_insert _ =
  with_heap_file (fun hf tid ->
    Storage.Heap_file.insert_tuple hf (U.counter_tuple 0) tid;
    Storage.Heap_file.insert_tuple hf (U.counter_tuple 1) tid;
    let tuples = Storage.Heap_file.scan_file hf tid |> List.of_seq in
    U.assert_int_eq 2 (List.length tuples);
    U.assert_tuple_eq (U.counter_tuple 0) (List.nth tuples 0);
    U.assert_tuple_eq (U.counter_tuple 1) (List.nth tuples 1))
;;

let test_delete _ =
  with_heap_file (fun hf tid ->
    Storage.Heap_file.insert_tuple hf (U.counter_tuple 1) tid;
    Storage.Heap_file.insert_tuple hf (U.counter_tuple 2) tid;
    let tuples = Storage.Heap_file.scan_file hf tid in
    Seq.iter (fun t -> Storage.Heap_file.delete_tuple hf t tid) tuples;
    let tuples = Storage.Heap_file.scan_file hf tid in
    U.assert_int_eq 0 (Seq.length tuples))
;;

let read_counter hf tid =
  let t = Storage.Heap_file.scan_file hf tid |> List.of_seq |> List.hd in
  Core.Value.to_int (Core.Tuple.attribute t 0), t
;;

let rec do_transaction task_id hf bp =
  try
    U.with_tid bp (fun tid ->
      Core.Log.log_tid tid "starting transaction for task %d" task_id;
      let n, t = read_counter hf tid in
      Storage.Heap_file.delete_tuple hf t tid;
      Storage.Heap_file.insert_tuple hf (U.counter_tuple (n + 1)) tid;
      Core.Log.log_tid tid "task %d ended" task_id)
  with
  | Core.Error.DBError Core.Error.Deadlock_victim ->
    Core.Log.log "deadlock detected on task %d, retrying..." task_id;
    Unix.sleepf 0.01;
    do_transaction task_id hf bp
;;

let test_concurrency num_domains num_trans _ =
  U.with_temp_heap_file U.counter_schema (fun hf bp ->
    Test_utils.with_tid bp (fun tid ->
      Storage.Heap_file.insert_tuple hf (U.counter_tuple 0) tid);
    let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
    T.run pool (fun _ ->
      List.init num_trans (fun task_id ->
        T.async pool (fun _ -> do_transaction task_id hf bp))
      |> List.iter (T.await pool));
    T.teardown_pool pool;
    let counter, _ = U.with_tid bp (read_counter hf) in
    U.assert_int_eq num_trans counter)
;;

let suite =
  "heap_file"
  >::: [ "insert" >:: test_insert
       ; "delete" >:: test_delete
       ; "concurrency 2d 5t" >:: test_concurrency 2 5
       ; "concurrency 2d 10t" >:: test_concurrency 2 10
       ; "concurrency 4d 20t" >:: test_concurrency 4 20
       ]
;;

let () = run_test_tt_main suite
