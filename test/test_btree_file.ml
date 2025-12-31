open OUnit2
module U = Test_utils

let test_insert _ =
  U.with_temp_btree_file U.counter_schema 0 (fun bt bp ->
    U.with_tid bp (fun tid ->
      let num_tuples = 512 in
      let sorted = List.init num_tuples U.make_counter_tuple in
      let shuffled = U.shuffle sorted in
      List.iter (fun t -> Storage.Btree_file.insert_tuple bt t tid) shuffled;
      let tuples =
        List.of_seq
          (Storage.Btree_file.range_scan
             bt
             (Core.Value_interval.unbounded Core.Type.Int)
             tid)
      in
      U.assert_int_eq num_tuples (List.length tuples);
      List.iter2 U.assert_tuple_eq sorted tuples))
;;

let test_delete _ =
  U.with_temp_btree_file U.counter_schema 0 (fun bt bp ->
    U.with_tid bp (fun tid ->
      let num_tuples = 512 in
      let num_deletions = 444 in
      let sorted = List.init num_tuples U.make_counter_tuple in
      let shuffled = U.shuffle sorted in
      List.iter (fun t -> Storage.Btree_file.insert_tuple bt t tid) shuffled;
      let deletions = List.take num_deletions shuffled in
      let remaining = List.drop num_deletions shuffled in
      let remaining_sorted = List.sort compare remaining in
      List.iter (fun t -> Storage.Btree_file.delete_tuple bt t tid) deletions;
      let tuples =
        List.of_seq
          (Storage.Btree_file.range_scan
             bt
             (Core.Value_interval.unbounded Core.Type.Int)
             tid)
      in
      U.assert_int_eq (num_tuples - num_deletions) (List.length tuples);
      List.iter2 U.assert_tuple_eq remaining_sorted tuples))
;;

let suite = "btree_file" >::: [ "insert" >:: test_insert; "delete" >:: test_delete ]
let () = run_test_tt_main suite
