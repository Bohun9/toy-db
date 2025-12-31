open Core
open Metadata

type t =
  { dir : string
  ; buf_pool : Storage.Buffer_pool.t
  ; table_registry : Table_registry.t
  }

let table_path cat tn = Printf.sprintf "%s/%s.tbl" cat.dir tn
let metatable_path cat tn = Printf.sprintf "%s/%s.cat" cat.dir tn
let tables_metatable_name = "_tables"
let columns_metatable_name = "_columns"
let tables_metatable_path cat = metatable_path cat "tables"
let columns_metatable_path cat = metatable_path cat "columns"

let tables_metatable_schema =
  Table_schema.create
    [ { name = "name"; typ = Type.String }; { name = "primary_key"; typ = Type.String } ]
    None
  |> Result.get_ok
;;

let columns_metatable_schema =
  Table_schema.create
    ([ "table", Type.String
     ; "name", Type.String
     ; "type_id", Type.Int
     ; "offset", Type.Int
     ]
     |> List.map (fun (name, typ) -> Syntax.{ name; typ }))
    None
  |> Result.get_ok
;;

type type_id = TypeId of int

let int_of_type_id (TypeId id) = id

let to_type_id = function
  | Type.Int -> TypeId 0
  | Type.String -> TypeId 1
;;

let from_type_id = function
  | TypeId 0 -> Type.Int
  | TypeId 1 -> Type.String
  | _ -> failwith "internal error - from_type_id"
;;

let encode_primary_key pk = Option.value pk ~default:""

let decode_primary_key = function
  | "" -> None
  | pk -> Some pk
;;

let remove_if_exists path = if Sys.file_exists path then Sys.remove path

let assert_table_not_exists cat name =
  if Table_registry.has_table cat.table_registry name
  then raise Error.table_already_exists
;;

let register_table cat tname schema ~clear =
  assert_table_not_exists cat tname;
  let file_path = table_path cat tname in
  if clear then remove_if_exists file_path;
  match Table_schema.primary_key schema with
  | None ->
    let hf = Storage.Heap_file.create file_path schema cat.buf_pool in
    Table_registry.add_table
      cat.table_registry
      tname
      (Db_file.TableFile (PackedTable ((module Storage.Heap_file), hf)))
  | Some (_, key_column_index) ->
    let bt = Storage.Btree_file.create file_path schema cat.buf_pool key_column_index in
    Table_registry.add_table
      cat.table_registry
      tname
      (Db_file.IndexFile (PackedIndex ((module Storage.Btree_file), bt)))
;;

let begin_new_transaction cat =
  let tid = Transaction_id.fresh_tid () in
  Storage.Buffer_pool.begin_transaction cat.buf_pool tid;
  tid
;;

let commit_transaction cat tid = Storage.Buffer_pool.commit_transaction cat.buf_pool tid

let with_tid cat f =
  let tid = begin_new_transaction cat in
  let r = f tid in
  commit_transaction cat tid;
  r
;;

type query_result =
  | Stream of Tuple.t Seq.t
  | Nothing

let execute_sql_stmt cat stmt tid =
  stmt
  |> Logical_plan.build_plan cat.table_registry
  |> Physical_plan.build_plan cat.table_registry
  |> Physical_plan.execute_plan tid
;;

let execute_sql_dml sql cat tid =
  match Sql_parser.parse sql with
  | Syntax.SQL_Stmt stmt -> ignore (execute_sql_stmt cat stmt tid |> List.of_seq)
  | Syntax.SQL_DDL _ -> failwith "internal error"
;;

let add_table cat name (schema : Syntax.table_schema) =
  let schema =
    match Table_schema.create schema.columns schema.primary_key with
    | Ok sch -> sch
    | Error err -> raise err
  in
  register_table cat name schema ~clear:false;
  with_tid cat (fun tid ->
    execute_sql_dml
      (Printf.sprintf
         "INSERT INTO %s VALUES (\"%s\", \"%s\")"
         tables_metatable_name
         name
         (Table_schema.primary_key schema |> Option.map fst |> encode_primary_key))
      cat
      tid;
    List.iteri
      (fun off Syntax.{ name = cname; typ } ->
         execute_sql_dml
           (Printf.sprintf
              "INSERT INTO %s VALUES (\"%s\", \"%s\", %s, %s)"
              columns_metatable_name
              name
              cname
              (to_type_id typ |> int_of_type_id |> string_of_int)
              (string_of_int off))
           cat
           tid)
      (Table_schema.columns schema))
;;

let delete_table cat name =
  let file_path = table_path cat name in
  remove_if_exists file_path;
  Table_registry.delete_table cat.table_registry name
;;

let execute_ddl_query cat = function
  | Syntax.CreateTable (name, schema) -> add_table cat name schema
  | Syntax.DropTable name -> delete_table cat name
;;

let execute_sql query cat tid =
  match Sql_parser.parse query with
  | Syntax.SQL_Stmt stmt -> execute_sql_stmt cat stmt tid |> fun s -> Stream s
  | Syntax.SQL_DDL ddl ->
    execute_ddl_query cat ddl;
    Nothing
;;

let get_table_names cat = Table_registry.get_table_names cat.table_registry

let get_table cat name =
  match Table_registry.get_table_opt cat.table_registry name with
  | Some t -> t
  | None -> raise Error.table_not_found
;;

let get_table_schema cat name = get_table cat name |> Db_file.schema
let sync_to_disk cat = Storage.Buffer_pool.flush_all_pages cat.buf_pool

let register_metatable cat name file schema =
  Table_registry.add_table
    cat.table_registry
    name
    (Db_file.TableFile
       (PackedTable
          ((module Storage.Heap_file), Storage.Heap_file.create file schema cat.buf_pool)))
;;

let register_metatables cat =
  register_metatable
    cat
    tables_metatable_name
    (tables_metatable_path cat)
    tables_metatable_schema;
  register_metatable
    cat
    columns_metatable_name
    (columns_metatable_path cat)
    columns_metatable_schema
;;

let load_catalog_tables cat =
  Log.log "catalog tables are loading...";
  let r =
    with_tid cat (fun tid ->
      match
        execute_sql (Printf.sprintf "SELECT * FROM %s" tables_metatable_name) cat tid
      with
      | Stream tables ->
        tables
        |> List.of_seq
        |> List.map (fun (t : Tuple.t) ->
          ( Tuple.attribute t 0 |> Value.value_to_string
          , Tuple.attribute t 1 |> Value.value_to_string |> decode_primary_key ))
      | _ -> failwith "internal error - load_catalog_tables")
  in
  Log.log "catalog tables loaded";
  r
;;

let load_catalog_columns cat =
  with_tid cat (fun tid ->
    match
      execute_sql (Printf.sprintf "SELECT * FROM %s" columns_metatable_name) cat tid
    with
    | Stream columns ->
      columns
      |> List.of_seq
      |> List.map (fun (t : Tuple.t) ->
        ( Tuple.attribute t 0 |> Value.value_to_string
        , Tuple.attribute t 1 |> Value.value_to_string
        , Tuple.attribute t 2 |> Value.value_to_int |> (fun t -> TypeId t) |> from_type_id
        , Tuple.attribute t 3 |> Value.value_to_int ))
    | _ -> failwith "internal error - load_catalog_columns")
;;

let load_catalog_metadata cat =
  let table_columns = Hashtbl.create 16 in
  let tables = load_catalog_tables cat in
  List.iter (fun (tname, _) -> Hashtbl.add table_columns tname []) tables;
  List.iter
    (fun (table, col, typ, off) ->
       match Hashtbl.find_opt table_columns table with
       | Some cols ->
         let new_cols = (off, Syntax.{ name = col; typ }) :: cols in
         Hashtbl.replace table_columns table new_cols
       | None -> failwith "internal error")
    (load_catalog_columns cat);
  List.iter
    (fun (tname, primary_key) ->
       let cols = Hashtbl.find table_columns tname in
       let sorted_cols = List.sort compare cols in
       let schema =
         List.map snd sorted_cols
         |> Fun.flip Table_schema.create primary_key
         |> Result.get_ok
       in
       register_table cat tname schema ~clear:false)
    tables
;;

let create dir bp_num_pages =
  let lock_manager = Storage.Lock_manager.create () in
  let buf_pool = Storage.Buffer_pool.create bp_num_pages lock_manager in
  let cat = { dir; buf_pool; table_registry = Table_registry.create 16 } in
  register_metatables cat;
  load_catalog_metadata cat;
  cat
;;

let delete_db_files cat =
  List.iter
    (fun (_, dbfile) -> Sys.remove (Db_file.file_path dbfile))
    (Table_registry.get_tables cat.table_registry)
;;
