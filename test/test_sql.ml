open OUnit2
open Toydb
module U = Test_utils
module C = Core
module V = C.Value

let with_letters_catalog f =
  U.with_temp_catalog (fun cat ->
    Catalog.with_tid cat (fun tid ->
      U.execute_ddl "CREATE TABLE letters (name STRING, quantity INT)" cat tid;
      U.execute_dml
        {|INSERT INTO letters VALUES ('A', 1), ('A', 2), ('B', 2), ('C', 3), ('B', 4), ('C', 5)|}
        cat
        tid;
      f cat tid))
;;

let test_group_by _ =
  with_letters_catalog (fun cat tid ->
    let result =
      U.execute_stmt
        "SELECT name, MIN(quantity) AS min, MAX(quantity) AS max, SUM(quantity) FROM \
         letters GROUP BY name"
        cat
        tid
    in
    let tuples = List.of_seq result.rows in
    U.assert_int_eq 3 (List.length tuples);
    U.assert_tuple_eq
      (C.Tuple.create [ V.String "A"; V.Int 1; V.Int 2; V.Int 3 ])
      (List.nth tuples 0);
    U.assert_tuple_eq
      (C.Tuple.create [ V.String "B"; V.Int 2; V.Int 4; V.Int 6 ])
      (List.nth tuples 1);
    U.assert_tuple_eq
      (C.Tuple.create [ V.String "C"; V.Int 3; V.Int 5; V.Int 8 ])
      (List.nth tuples 2))
;;

let test_order_by _ =
  with_letters_catalog (fun cat tid ->
    let result =
      U.execute_stmt
        "SELECT * FROM letters ORDER BY name ASC, letters.quantity DESC"
        cat
        tid
    in
    let tuples = List.of_seq result.rows in
    U.assert_int_eq 6 (List.length tuples);
    U.assert_tuple_eq (C.Tuple.create [ V.String "A"; V.Int 2 ]) (List.nth tuples 0);
    U.assert_tuple_eq (C.Tuple.create [ V.String "A"; V.Int 1 ]) (List.nth tuples 1);
    U.assert_tuple_eq (C.Tuple.create [ V.String "B"; V.Int 4 ]) (List.nth tuples 2);
    U.assert_tuple_eq (C.Tuple.create [ V.String "B"; V.Int 2 ]) (List.nth tuples 3);
    U.assert_tuple_eq (C.Tuple.create [ V.String "C"; V.Int 5 ]) (List.nth tuples 4);
    U.assert_tuple_eq (C.Tuple.create [ V.String "C"; V.Int 3 ]) (List.nth tuples 5))
;;

let test_limit _ =
  with_letters_catalog (fun cat tid ->
    let result =
      U.execute_stmt
        "SELECT * FROM letters ORDER BY name ASC, letters.quantity DESC LIMIT 2 OFFSET 1"
        cat
        tid
    in
    let tuples = List.of_seq result.rows in
    U.assert_int_eq 2 (List.length tuples);
    U.assert_tuple_eq (C.Tuple.create [ V.String "A"; V.Int 1 ]) (List.nth tuples 0);
    U.assert_tuple_eq (C.Tuple.create [ V.String "B"; V.Int 4 ]) (List.nth tuples 1))
;;

let test_subquery _ =
  with_letters_catalog (fun cat tid ->
    let result =
      U.execute_stmt
        "SELECT * FROM letters JOIN (SELECT MAX(quantity) FROM letters GROUP BY) ON \
         quantity = max"
        cat
        tid
    in
    let tuples = List.of_seq result.rows in
    U.assert_int_eq 1 (List.length tuples);
    U.assert_tuple_eq
      (C.Tuple.create [ V.String "C"; V.Int 5; V.Int 5 ])
      (List.nth tuples 0))
;;

let suite =
  "sql"
  >::: [ "group_by" >:: test_group_by
       ; "order_by" >:: test_order_by
       ; "limit" >:: test_limit
       ; "subquery" >:: test_subquery
       ]
;;

let () = run_test_tt_main suite
