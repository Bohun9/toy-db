open OUnit2
open Toydb
module U = Test_utils

let test_persistence _ =
  U.with_temp_dir "temp_db_catalog_" "" (fun dir ->
    U.with_catalog dir (fun cat ->
      Catalog.with_tid cat (fun tid ->
        Catalog.execute_sql "CREATE TABLE numbers (n INT. PRIMARY KEY (n))" cat tid
        |> ignore;
        Catalog.execute_sql "INSERT INTO numbers VALUES (0), (1), (2)" cat tid |> ignore));
    U.with_catalog dir (fun cat ->
      Catalog.with_tid cat (fun tid ->
        match Catalog.execute_sql "SELECT * FROM numbers" cat tid with
        | Catalog.Rows { rows; _ } ->
          let tuples = List.of_seq rows in
          U.assert_int_eq 3 (List.length tuples);
          U.assert_tuple_eq (U.make_counter_tuple 0) (List.nth tuples 0);
          U.assert_tuple_eq (U.make_counter_tuple 1) (List.nth tuples 1);
          U.assert_tuple_eq (U.make_counter_tuple 2) (List.nth tuples 2)
        | Catalog.NoResult -> failwith "internal error");
      Catalog.delete_db_files cat))
;;

let suite = "catalog" >::: [ "persistence" >:: test_persistence ]
let () = run_test_tt_main suite
