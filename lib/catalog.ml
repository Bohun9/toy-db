module C = Core
module M = Metadata
module S = Storage

type t =
  { dir : string
  ; buf_pool : S.Buffer_pool.t
  ; table_registry : M.Table_registry.t
  ; metatable_registry : M.Table_registry.t
  }

let table_path cat name = Printf.sprintf "%s/%s.tbl" cat.dir name
let metatable_path cat name = Printf.sprintf "%s/%s.cat" cat.dir name
let tables_metatable_name = "tables"
let columns_metatable_name = "columns"

let tables_metatable_schema =
  M.Table_schema.create
    [ { name = "name"; typ = C.Type.String }
    ; { name = "primary_key"; typ = C.Type.String }
    ]
    None
;;

let columns_metatable_schema =
  M.Table_schema.create
    [ { name = "table"; typ = C.Type.String }
    ; { name = "name"; typ = C.Type.String }
    ; { name = "type"; typ = C.Type.String }
    ; { name = "offset"; typ = C.Type.Int }
    ]
    None
;;

let encode_type = function
  | C.Type.Int -> "Int"
  | C.Type.String -> "String"
;;

let decode_type = function
  | "Int" -> C.Type.Int
  | "String" -> C.Type.String
  | _ -> failwith "internal error"
;;

let encode_primary_key pk = Option.value pk ~default:""

let decode_primary_key = function
  | "" -> None
  | pk -> Some pk
;;

let begin_new_transaction cat =
  let tid = C.Transaction_id.fresh_tid () in
  S.Buffer_pool.begin_transaction cat.buf_pool tid;
  tid
;;

let commit_transaction cat tid = S.Buffer_pool.commit_transaction cat.buf_pool tid

let with_tid cat f =
  let tid = begin_new_transaction cat in
  let r = f tid in
  commit_transaction cat tid;
  r
;;

let remove_if_exists path = if Sys.file_exists path then Sys.remove path

let require_table_not_exists cat name =
  if M.Table_registry.has_table cat.table_registry name
  then raise M.Error.table_already_exists
;;

let register_table cat name schema ~clear =
  require_table_not_exists cat name;
  let file_path = table_path cat name in
  if clear then remove_if_exists file_path;
  let db_file =
    match M.Table_schema.primary_key schema with
    | None ->
      let hf = S.Heap_file.create file_path schema cat.buf_pool in
      M.Db_file.TableFile (PackedTable ((module S.Heap_file), hf))
    | Some (_, key_column_index) ->
      let bt = S.Btree_file.create file_path schema cat.buf_pool key_column_index in
      M.Db_file.IndexFile (PackedIndex ((module S.Btree_file), bt))
  in
  M.Table_registry.add_table cat.table_registry name db_file
;;

let register_metatable cat name schema =
  let hf = S.Heap_file.create (metatable_path cat name) schema cat.buf_pool in
  M.Table_registry.add_table
    cat.metatable_registry
    name
    (M.Db_file.TableFile (PackedTable ((module S.Heap_file), hf)))
;;

let register_metatables cat =
  register_metatable cat tables_metatable_name tables_metatable_schema;
  register_metatable cat columns_metatable_name columns_metatable_schema
;;

type rows_info =
  { desc : Physical_plan.Tuple_desc.t
  ; rows : C.Tuple.t Seq.t
  }

type query_result =
  | Rows of rows_info
  | NoResult

let execute_sql_stmt reg stmt tid =
  let pp = stmt |> Logical_plan.build_plan reg |> Physical_plan.build_plan reg in
  { desc = pp.desc; rows = Physical_plan.execute_plan tid pp }
;;

let execute_meta_query sql cat tid =
  match Sql_parser.parse sql with
  | C.Syntax.SQL_Stmt stmt ->
    (execute_sql_stmt cat.metatable_registry stmt tid).rows |> List.of_seq
  | _ -> failwith "internal error"
;;

let execute_meta_dml sql cat tid = execute_meta_query sql cat tid |> ignore

let add_table cat name (schema : C.Syntax.table_schema) tid =
  let schema = M.Table_schema.create schema.columns schema.primary_key in
  register_table cat name schema ~clear:true;
  execute_meta_dml
    (Printf.sprintf
       {|INSERT INTO %s VALUES ('%s', '%s')|}
       tables_metatable_name
       name
       (M.Table_schema.primary_key schema |> Option.map fst |> encode_primary_key))
    cat
    tid;
  List.iteri
    (fun off C.Syntax.{ name = cname; typ } ->
       execute_meta_dml
         (Printf.sprintf
            {|INSERT INTO %s VALUES ('%s', '%s', '%s', %d)|}
            columns_metatable_name
            name
            cname
            (encode_type typ)
            off)
         cat
         tid)
    (M.Table_schema.columns schema)
;;

let delete_table cat name =
  M.Table_registry.delete_table cat.table_registry name;
  remove_if_exists (table_path cat name)
;;

let execute_ddl_query cat ddl tid =
  match ddl with
  | C.Syntax.CreateTable (name, schema) -> add_table cat name schema tid
  | C.Syntax.DropTable name -> delete_table cat name
;;

let execute_sql query cat tid =
  match Sql_parser.parse query with
  | C.Syntax.SQL_Stmt stmt -> Rows (execute_sql_stmt cat.table_registry stmt tid)
  | C.Syntax.SQL_DDL ddl ->
    execute_ddl_query cat ddl tid;
    NoResult
;;

let load_tables_metatable cat =
  with_tid cat (fun tid ->
    execute_meta_query (Printf.sprintf "SELECT * FROM %s" tables_metatable_name) cat tid
    |> List.map (fun (t : C.Tuple.t) ->
      ( C.Tuple.attribute t 0 |> C.Value.to_string
      , C.Tuple.attribute t 1 |> C.Value.to_string |> decode_primary_key )))
;;

let load_columns_metatable cat =
  with_tid cat (fun tid ->
    execute_meta_query
      (Printf.sprintf "SELECT * FROM %s ORDER BY offset ASC" columns_metatable_name)
      cat
      tid
    |> List.map (fun (t : C.Tuple.t) ->
      ( C.Tuple.attribute t 0 |> C.Value.to_string
      , C.Tuple.attribute t 1 |> C.Value.to_string
      , C.Tuple.attribute t 2 |> C.Value.to_string |> decode_type )))
;;

let register_tables cat =
  let tables = load_tables_metatable cat in
  let columns = load_columns_metatable cat in
  List.iter
    (fun (tname, primary_key) ->
       let schema =
         columns
         |> List.filter (fun (tname', _, _) -> tname = tname')
         |> List.map (fun (_, name, typ) -> C.Syntax.{ name; typ })
         |> Fun.flip M.Table_schema.create primary_key
       in
       register_table cat tname schema ~clear:false)
    tables
;;

let create dir bp_num_pages =
  let lock_manager = S.Lock_manager.create () in
  let buf_pool = S.Buffer_pool.create bp_num_pages lock_manager in
  let cat =
    { dir
    ; buf_pool
    ; table_registry = M.Table_registry.create 16
    ; metatable_registry = M.Table_registry.create 2
    }
  in
  register_metatables cat;
  register_tables cat;
  cat
;;

let delete_db_files cat =
  List.iter
    (fun (_, dbfile) -> Sys.remove (M.Db_file.file_path dbfile))
    (M.Table_registry.get_tables cat.table_registry
     @ M.Table_registry.get_tables cat.metatable_registry)
;;

let get_table_names cat = M.Table_registry.get_table_names cat.table_registry
let get_table cat name = M.Table_registry.get_table cat.table_registry name
let get_table_schema cat name = get_table cat name |> M.Db_file.schema
