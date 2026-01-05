open OUnit2
module T = Domainslib.Task
module U = Test_utils
module C = Core
module M = Metadata
module S = Storage

let jumbo_length =
  (S.Storage_layout.Page_io.page_size - (1 + 8 + 8 + 2)) / (3 * C.Value.string_max_length)
;;

let jumbo_schema =
  M.Table_schema.create
    (List.init jumbo_length (fun i ->
       C.Syntax.{ name = Printf.sprintf "data%d" i; typ = C.Type.String }))
    None
;;

let jumbo_tuple n =
  C.Tuple.create
    (C.Value.String (string_of_int n)
     :: List.init (jumbo_length - 1) (fun _ -> C.Value.String ""))
;;

let jumbo_unbounded_interval = C.Value_interval.unbounded C.Type.String
let sorted_jumbos n = List.init n jumbo_tuple |> List.sort C.Value.compare

let test_jumbo_schema _ =
  U.assert_int_eq 3 (S.Btree_leaf_page.max_num_tuples_for_schema jumbo_schema)
;;

let with_btree f =
  U.with_temp_btree_file jumbo_schema 0 (fun bt bp -> U.with_tid bp (f bt))
;;

let num_tuples = 1024
let num_deletions = 999

let test_insert _ =
  with_btree (fun bt tid ->
    let sorted = sorted_jumbos num_tuples in
    let shuffled = U.shuffle sorted in
    List.iter (fun t -> S.Btree_file.insert_tuple bt t tid) shuffled;
    let tuples = S.Btree_file.range_scan bt jumbo_unbounded_interval tid |> List.of_seq in
    U.assert_int_eq num_tuples (List.length tuples);
    List.iter2 U.assert_tuple_eq sorted tuples)
;;

let test_delete _ =
  with_btree (fun bt tid ->
    let shuffled = U.shuffle (sorted_jumbos num_tuples) in
    List.iter (fun t -> S.Btree_file.insert_tuple bt t tid) shuffled;
    let deletions = List.take num_deletions shuffled in
    let remaining_sorted =
      List.drop num_deletions shuffled |> List.sort C.Value.compare
    in
    List.iter (fun t -> S.Btree_file.delete_tuple bt t tid) deletions;
    let tuples = S.Btree_file.range_scan bt jumbo_unbounded_interval tid |> List.of_seq in
    U.assert_int_eq (num_tuples - num_deletions) (List.length tuples);
    List.iter2 U.assert_tuple_eq remaining_sorted tuples)
;;

let rec do_transaction task_id bt bp =
  try
    U.with_tid bp (fun tid ->
      Core.Log.log_tid tid "starting transaction for task %d" task_id;
      S.Btree_file.insert_tuple bt (jumbo_tuple task_id) tid;
      S.Btree_file.delete_tuple bt (jumbo_tuple task_id) tid;
      Core.Log.log_tid tid "task %d ended" task_id)
  with
  | Core.Error.DBError Core.Error.Deadlock_victim ->
    Core.Log.log "deadlock detected on task %d, retrying..." task_id;
    Unix.sleepf 0.01;
    do_transaction task_id bt bp
;;

let test_concurrency num_domains num_trans _ =
  U.with_temp_btree_file jumbo_schema 0 (fun bt bp ->
    let pool = T.setup_pool ~num_domains:(num_domains - 1) () in
    T.run pool (fun _ ->
      List.init num_trans (fun task_id ->
        T.async pool (fun _ -> do_transaction task_id bt bp))
      |> List.iter (T.await pool));
    T.teardown_pool pool;
    let tuples =
      U.with_tid bp (S.Btree_file.range_scan bt jumbo_unbounded_interval) |> List.of_seq
    in
    U.assert_int_eq 0 (List.length tuples))
;;

let suite =
  "btree_file"
  >::: [ "jumbo schema" >:: test_jumbo_schema
       ; "insert" >:: test_insert
       ; "delete" >:: test_delete
       ; "concurrency 2d 100t" >:: test_concurrency 2 100
       ; "concurrency 2d 300t" >:: test_concurrency 2 300
       ; "concurrency 4d 300t" >:: test_concurrency 4 300
       ]
;;

let () = run_test_tt_main suite
