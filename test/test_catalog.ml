open OUnit2
open Toydb
module U = Test_utils

let test_persistence _ =
  U.with_temp_dir (fun dir ->
    U.with_catalog dir (fun cat ->
      Catalog.with_tid cat (fun tid ->
        U.execute_ddl "CREATE TABLE numbers (n INT. PRIMARY KEY (n))" cat tid;
        U.execute_dml "INSERT INTO numbers VALUES (0), (1), (2)" cat tid));
    U.with_catalog dir (fun cat ->
      Catalog.with_tid cat (fun tid ->
        let result = U.execute_stmt "SELECT * FROM numbers" cat tid in
        let tuples = List.of_seq result.rows in
        U.assert_int_eq 3 (List.length tuples);
        U.assert_tuple_eq (U.counter_tuple 0) (List.nth tuples 0);
        U.assert_tuple_eq (U.counter_tuple 1) (List.nth tuples 1);
        U.assert_tuple_eq (U.counter_tuple 2) (List.nth tuples 2));
      Catalog.delete_db_files cat))
;;

let suite = "catalog" >::: [ "persistence" >:: test_persistence ]
let () = run_test_tt_main suite
