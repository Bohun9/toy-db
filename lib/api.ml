let create_catalog dir cache_size =
  let buf_pool = Buffer_pool.create cache_size in
  let cat = Catalog.create dir buf_pool in
  let desc =
    [ Tuple.FieldMetadata { name = Syntax.PureFieldName "number"; typ = Tuple.TInt } ]
  in
  Catalog.add_table cat "numbers" desc ~clear:false;
  (* let (Catalog.PackedDBFile (m, f)) = Catalog.get_table cat "numbers" in *)
  (* let module M = (val m) in *)
  (* M.insert_tuple f (Tuple.Tuple { desc; values = [ Tuple.VInt 1 ]; rid = None }); *)
  (* M.insert_tuple f (Tuple.Tuple { desc; values = [ Tuple.VInt 2 ]; rid = None }); *)
  (* M.insert_tuple f (Tuple.Tuple { desc; values = [ Tuple.VInt 3 ]; rid = None }); *)
  cat
;;

type query_result =
  | Stream of Tuple.tuple Seq.t
  | Nothing

let execute_sql sql_query cat =
  match Parser_wrapper.parse_sql sql_query with
  | SQL_DML dml ->
    (* print_endline "hello"; *)
    dml |> Physical_plan.make_plan cat |> Physical_plan.execute_plan |> fun s -> Stream s
  | SQL_DDL ddl ->
    Catalog.process_ddl_query cat ddl;
    Nothing
;;
