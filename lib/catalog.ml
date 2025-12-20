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
let catalog_tables_desc = [ Tuple.field_metadata "name" Tuple.TString ]

let catalog_columns_desc =
  [ Tuple.field_metadata "table" Tuple.TString
  ; Tuple.field_metadata "name" Tuple.TString
  ; Tuple.field_metadata "type_id" Tuple.TInt
  ; Tuple.field_metadata "offset" Tuple.TInt
  ]
;;

type type_id = TypeId of int

let int_of_type_id (TypeId id) = id

let to_type_id = function
  | Tuple.TInt -> TypeId 0
  | Tuple.TString -> TypeId 1
  | _ -> failwith "internal error - to_type_id"
;;

let from_type_id = function
  | TypeId 0 -> Tuple.TInt
  | TypeId 1 -> Tuple.TString
  | _ -> failwith "internal error - from_type_id"
;;

let remove_if_exists path = if Sys.file_exists path then Sys.remove path

let assert_table_not_exists cat name =
  if Table_registry.has_table cat.table_registry name
  then raise Error.table_already_exists
;;

let register_table cat name desc ~clear =
  assert_table_not_exists cat name;
  let file_path = table_path cat name in
  if clear then remove_if_exists file_path;
  let hf = Heap_file.create file_path desc cat.buf_pool in
  Table_registry.add_table cat.table_registry name (module Heap_file) hf
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
  dml |> Physical_plan.make_plan cat.table_registry |> Physical_plan.execute_plan tid
;;

let execute_dml_sql query cat tid =
  match Parser_wrapper.parse_sql query with
  | SQL_DML dml ->
    ignore (execute_dml_query cat dml tid |> List.of_seq);
    ()
  | SQL_DDL _ -> failwith "internal error"
;;

let add_table cat name desc =
  register_table cat name desc ~clear:false;
  with_tid cat (fun tid ->
    execute_dml_sql
      (Printf.sprintf "INSERT INTO %s VALUES (\"%s\")" catalog_tables_name name)
      cat
      tid;
    List.iteri
      (fun off (Tuple.FieldMetadata { name = fname; typ }) ->
        match fname with
        | Syntax.PureFieldName cname ->
          execute_dml_sql
            (Printf.sprintf
               "INSERT INTO %s VALUES (\"%s\", \"%s\", %s, %s)"
               catalog_columns_name
               name
               cname
               (to_type_id typ |> int_of_type_id |> string_of_int)
               (string_of_int off))
            cat
            tid
        | _ -> failwith "internal error")
      desc)
;;

let delete_table cat name =
  let file_path = table_path cat name in
  remove_if_exists file_path;
  Table_registry.delete_table cat.table_registry name
;;

let execute_ddl_query cat = function
  | Syntax.CreateTable (name, schema) ->
    let desc = Tuple.trans_table_schema schema in
    add_table cat name desc
  | Syntax.DropTable name -> delete_table cat name
;;

let execute_sql query cat tid =
  match Parser_wrapper.parse_sql query with
  | SQL_DML dml -> execute_dml_query cat dml tid |> fun s -> Stream s
  | SQL_DDL ddl ->
    execute_ddl_query cat ddl;
    Nothing
;;

let get_table_names cat = Table_registry.get_table_names cat.table_registry

let get_table_desc cat name =
  let (PackedDBFile (m, f)) = Table_registry.get_table cat.table_registry name in
  let module M = (val m) in
  M.desc f
;;

let sync_to_disk cat = Buffer_pool.flush_all_pages cat.buf_pool

let register_metatable cat name file desc =
  Table_registry.add_table
    cat.table_registry
    name
    (module Heap_file)
    (Heap_file.create file desc cat.buf_pool)
;;

let register_metatables cat =
  register_metatable cat catalog_tables_name (catalog_tables_path cat) catalog_tables_desc;
  register_metatable
    cat
    catalog_columns_name
    (catalog_columns_path cat)
    catalog_columns_desc
;;

let load_catalog_tables cat =
  with_tid cat (fun tid ->
    match execute_sql (Printf.sprintf "SELECT * FROM %s" catalog_tables_name) cat tid with
    | Stream tables ->
      tables
      |> List.of_seq
      |> List.map (fun (t : Tuple.t) ->
        match List.hd t.values with
        | Tuple.VString t -> t
        | _ -> failwith "internal error - load_catalog_tables")
    | _ -> failwith "internal error - load_catalog_tables")
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
        ( List.nth t.values 0 |> Tuple.value_to_string
        , List.nth t.values 1 |> Tuple.value_to_string
        , List.nth t.values 2 |> Tuple.value_to_int |> (fun t -> TypeId t) |> from_type_id
        , List.nth t.values 3 |> Tuple.value_to_int ))
    | _ -> failwith "internal error - load_catalog_columns")
;;

let load_catalog_metadata cat =
  let table_columns = Hashtbl.create 16 in
  List.iter (fun table -> Hashtbl.add table_columns table []) (load_catalog_tables cat);
  List.iter
    (fun (table, col, typ, off) ->
      match Hashtbl.find_opt table_columns table with
      | Some cols ->
        let new_cols = (off, Tuple.field_metadata col typ) :: cols in
        Hashtbl.replace table_columns table new_cols
      | None -> failwith "internal error")
    (load_catalog_columns cat);
  Hashtbl.iter
    (fun table cols ->
      let sorted_cols = List.sort compare cols in
      let desc = List.map snd sorted_cols in
      register_table cat table desc ~clear:false)
    table_columns
;;

let create dir buf_pool =
  let cat = { dir; buf_pool; table_registry = Table_registry.create 16 } in
  register_metatables cat;
  load_catalog_metadata cat;
  cat
;;
