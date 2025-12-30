open OUnit2
open Toydb
module U = Test_utils
module T = Core.Tuple
module V = Core.Value

let with_filled_catalog f =
  U.with_temp_catalog (fun cat ->
    Catalog.with_tid cat (fun tid ->
      Catalog.execute_sql "CREATE TABLE letters (name STRING, quantity INT)" cat tid
      |> ignore;
      Catalog.execute_sql
        {|INSERT INTO letters VALUES ("A", 1), ("A", 2), ("B", 2), ("C", 3), ("B", 4), ("C", 5)|}
        cat
        tid
      |> ignore);
    f cat)
;;

let test_group_by _ =
  with_filled_catalog (fun cat ->
    Catalog.with_tid cat (fun tid ->
      let result =
        Catalog.execute_sql
          "SELECT name, MIN(quantity) AS min, MAX(quantity) AS max, SUM(quantity) AS sum \
           FROM letters GROUP BY name"
          cat
          tid
      in
      match result with
      | Catalog.Stream tuples ->
        let tuples = List.of_seq tuples in
        U.assert_int_eq 3 (List.length tuples);
        U.assert_tuple_eq
          T.{ values = [ V.VString "A"; V.VInt 1; V.VInt 2; V.VInt 3 ]; rid = None }
          (List.nth tuples 0);
        U.assert_tuple_eq
          T.{ values = [ V.VString "B"; V.VInt 2; V.VInt 4; V.VInt 6 ]; rid = None }
          (List.nth tuples 1);
        U.assert_tuple_eq
          T.{ values = [ V.VString "C"; V.VInt 3; V.VInt 5; V.VInt 8 ]; rid = None }
          (List.nth tuples 2)
      | Catalog.Nothing -> failwith "internal error"))
;;

let test_order_by _ =
  with_filled_catalog (fun cat ->
    Catalog.with_tid cat (fun tid ->
      let result =
        Catalog.execute_sql
          "SELECT * FROM letters ORDER BY name ASC, letters.quantity DESC"
          cat
          tid
      in
      match result with
      | Catalog.Stream tuples ->
        let tuples = List.of_seq tuples in
        U.assert_int_eq 6 (List.length tuples);
        U.assert_tuple_eq
          T.{ values = [ V.VString "A"; V.VInt 2 ]; rid = None }
          (List.nth tuples 0);
        U.assert_tuple_eq
          T.{ values = [ V.VString "A"; V.VInt 1 ]; rid = None }
          (List.nth tuples 1);
        U.assert_tuple_eq
          T.{ values = [ V.VString "B"; V.VInt 4 ]; rid = None }
          (List.nth tuples 2);
        U.assert_tuple_eq
          T.{ values = [ V.VString "B"; V.VInt 2 ]; rid = None }
          (List.nth tuples 3);
        U.assert_tuple_eq
          T.{ values = [ V.VString "C"; V.VInt 5 ]; rid = None }
          (List.nth tuples 4);
        U.assert_tuple_eq
          T.{ values = [ V.VString "C"; V.VInt 3 ]; rid = None }
          (List.nth tuples 5)
      | Catalog.Nothing -> failwith "internal error"))
;;

let test_limit _ =
  with_filled_catalog (fun cat ->
    Catalog.with_tid cat (fun tid ->
      let result =
        Catalog.execute_sql
          "SELECT * FROM letters ORDER BY name ASC, letters.quantity DESC LIMIT 2 OFFSET \
           1"
          cat
          tid
      in
      match result with
      | Catalog.Stream tuples ->
        let tuples = List.of_seq tuples in
        U.assert_int_eq 2 (List.length tuples);
        U.assert_tuple_eq
          T.{ values = [ V.VString "A"; V.VInt 1 ]; rid = None }
          (List.nth tuples 0);
        U.assert_tuple_eq
          T.{ values = [ V.VString "B"; V.VInt 4 ]; rid = None }
          (List.nth tuples 1)
      | Catalog.Nothing -> failwith "internal error"))
;;

let test_subquery _ =
  with_filled_catalog (fun cat ->
    Catalog.with_tid cat (fun tid ->
      let result =
        Catalog.execute_sql
          "SELECT * FROM letters JOIN (SELECT MAX(quantity) AS max FROM letters GROUP \
           BY) AS dummy ON quantity = max"
          cat
          tid
      in
      match result with
      | Catalog.Stream tuples ->
        let tuples = List.of_seq tuples in
        U.assert_int_eq 1 (List.length tuples);
        U.assert_tuple_eq
          T.{ values = [ V.VString "C"; V.VInt 5; V.VInt 5 ]; rid = None }
          (List.nth tuples 0)
      | Catalog.Nothing -> failwith "internal error"))
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
