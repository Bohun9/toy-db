type t =
  { dir : string
  ; buf_pool : Buffer_pool.t
  ; table_registry : Table_registry.t
  }

let catalog_tables_name = "_tables"
let catalog_columns_name = "_columns"
let table_path cat tn = cat.dir ^ "/" ^ tn ^ ".tbl"
let catalog_tables_path cat = cat.dir ^ "/tables.cat"
let catalog_columns_path cat = cat.dir ^ "/columns.cat"
let catalog_tables_schema = Table_schema.from_list [ "name", Type.TString ]

let catalog_columns_schema =
  Table_schema.from_list
    [ "table", Type.TString
    ; "name", Type.TString
    ; "type_id", Type.TInt
    ; "offset", Type.TInt
    ]
;;

type type_id = TypeId of int

let int_of_type_id (TypeId id) = id

let to_type_id = function
  | Type.TInt -> TypeId 0
  | Type.TString -> TypeId 1
;;

let from_type_id = function
  | TypeId 0 -> Type.TInt
  | TypeId 1 -> Type.TString
  | _ -> failwith "internal error - from_type_id"
;;

let remove_if_exists path = if Sys.file_exists path then Sys.remove path

let assert_table_not_exists cat name =
  if Table_registry.has_table cat.table_registry name
  then raise Error.table_already_exists
;;

let register_table cat name schema ~clear =
  assert_table_not_exists cat name;
  let file_path = table_path cat name in
  if clear then remove_if_exists file_path;
  let hf = Heap_file.create file_path schema cat.buf_pool in
  Table_registry.add_table
    cat.table_registry
    name
    (Packed_dbfile.TableFile (PackedTable ((module Heap_file), hf)))
;;

let begin_new_transaction cat =
  let tid = Transaction_id.fresh_tid () in
  Buffer_pool.begin_transaction cat.buf_pool tid;
  tid
;;

let commit_transaction cat tid = Buffer_pool.commit_transaction cat.buf_pool tid

let with_tid cat f =
  let tid = begin_new_transaction cat in
  let res = f tid in
  commit_transaction cat tid;
  res
;;

type query_result =
  | Stream of Tuple.t Seq.t
  | Nothing

let execute_dml_query cat dml tid =
  Log.log "execute dml query";
  dml
  |> Logical_plan.build_plan cat.table_registry
  |> Physical_plan.build_plan cat.table_registry
  |> Physical_plan.execute_plan tid
;;

let execute_dml_sql query cat tid =
  match Parser_wrapper.parse_sql query with
  | SQL_Stmt stmt ->
    ignore (execute_dml_query cat stmt tid |> List.of_seq);
    ()
  | SQL_DDL _ -> failwith "internal error"
;;

let add_table cat name schema =
  register_table cat name schema ~clear:false;
  with_tid cat (fun tid ->
    execute_dml_sql
      (Printf.sprintf "INSERT INTO %s VALUES (\"%s\")" catalog_tables_name name)
      cat
      tid;
    List.iteri
      (fun off Table_schema.{ name = cname; typ } ->
         execute_dml_sql
           (Printf.sprintf
              "INSERT INTO %s VALUES (\"%s\", \"%s\", %s, %s)"
              catalog_columns_name
              name
              cname
              (to_type_id typ |> int_of_type_id |> string_of_int)
              (string_of_int off))
           cat
           tid)
      schema)
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
  match Parser_wrapper.parse_sql query with
  | SQL_Stmt stmt -> execute_dml_query cat stmt tid |> fun s -> Stream s
  | SQL_DDL ddl ->
    execute_ddl_query cat ddl;
    Nothing
;;

let get_table_names cat = Table_registry.get_table_names cat.table_registry

let get_table cat name =
  match Table_registry.get_table_opt cat.table_registry name with
  | Some t -> t
  | None -> raise Error.table_not_found
;;

let get_table_schema cat name = get_table cat name |> Packed_dbfile.schema
let sync_to_disk cat = Buffer_pool.flush_all_pages cat.buf_pool

let register_metatable cat name file schema =
  Table_registry.add_table
    cat.table_registry
    name
    (Packed_dbfile.TableFile
       (PackedTable ((module Heap_file), Heap_file.create file schema cat.buf_pool)))
;;

let register_metatables cat =
  register_metatable
    cat
    catalog_tables_name
    (catalog_tables_path cat)
    catalog_tables_schema;
  register_metatable
    cat
    catalog_columns_name
    (catalog_columns_path cat)
    catalog_columns_schema
;;

let load_catalog_tables cat =
  Log.log "catalog tables are loading...";
  let r =
    with_tid cat (fun tid ->
      match
        execute_sql (Printf.sprintf "SELECT * FROM %s" catalog_tables_name) cat tid
      with
      | Stream tables ->
        tables
        |> List.of_seq
        |> List.map (fun (t : Tuple.t) -> List.hd t.values |> Value.value_to_string)
      | _ -> failwith "internal error - load_catalog_tables")
  in
  Log.log "catalog tables loaded";
  r
;;

let load_catalog_columns cat =
  with_tid cat (fun tid ->
    match
      execute_sql (Printf.sprintf "SELECT * FROM %s" catalog_columns_name) cat tid
    with
    | Stream columns ->
      columns
      |> List.of_seq
      |> List.map (fun (t : Tuple.t) ->
        ( List.nth t.values 0 |> Value.value_to_string
        , List.nth t.values 1 |> Value.value_to_string
        , List.nth t.values 2 |> Value.value_to_int |> (fun t -> TypeId t) |> from_type_id
        , List.nth t.values 3 |> Value.value_to_int ))
    | _ -> failwith "internal error - load_catalog_columns")
;;

let load_catalog_metadata cat =
  let table_columns = Hashtbl.create 16 in
  List.iter (fun table -> Hashtbl.add table_columns table []) (load_catalog_tables cat);
  List.iter
    (fun (table, col, typ, off) ->
       match Hashtbl.find_opt table_columns table with
       | Some cols ->
         let new_cols = (off, (col, typ)) :: cols in
         Hashtbl.replace table_columns table new_cols
       | None -> failwith "internal error")
    (load_catalog_columns cat);
  Hashtbl.iter
    (fun table cols ->
       let sorted_cols = List.sort compare cols in
       let schema = List.map snd sorted_cols |> Table_schema.from_list in
       register_table cat table schema ~clear:false)
    table_columns
;;

let create dir buf_pool =
  Log.log "create catalog...";
  let cat = { dir; buf_pool; table_registry = Table_registry.create 16 } in
  register_metatables cat;
  Log.log "loading catalog metadata...";
  load_catalog_metadata cat;
  cat
;;
