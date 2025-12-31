open OUnit2
module T = Domainslib.Task
module U = Test_utils

(* Basic test for parallel transactions incrementing a counter in a heap file *)

let read_counter hf tid =
  let t = Storage.Heap_file.scan_file hf tid |> List.of_seq |> List.hd in
  Core.Value.value_to_int (Core.Tuple.attribute t 0), t.rid
;;

let rec do_transaction task_id hf bp =
  try
    U.with_tid bp (fun tid ->
      Core.Log.log_tid tid "starting transaction for task %d" task_id;
      let n, rid = read_counter hf tid in
      Storage.Heap_file.delete_tuple hf rid tid;
      Storage.Heap_file.insert_tuple hf (U.make_counter_tuple (n + 1)) tid;
      Core.Log.log_tid tid "task %d ended" task_id)
  with
  | Core.Error.DBError Core.Error.Deadlock_victim ->
    Core.Log.log "deadlock detected on task %d, retrying..." task_id;
    Unix.sleepf 0.01;
    do_transaction task_id hf bp
;;

let test_parallel num_domains num_trans _ =
  U.with_temp_heap_file U.counter_schema (fun hf bp ->
    Test_utils.with_tid bp (fun tid ->
      Storage.Heap_file.insert_tuple hf (U.make_counter_tuple 0) tid);
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
  "transaction"
  >::: [ "parallel 2d 5t" >:: test_parallel 2 5
       ; "parallel 2d 10t" >:: test_parallel 2 10
       ; "parallel 4d 20t" >:: test_parallel 4 20
       ]
;;

let () = run_test_tt_main suite
