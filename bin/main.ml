open Toydb
open Printf

let catalog = Catalog.create "." 128

let rec loop tid =
  printf "=> ";
  flush stdout;
  match String.trim (read_line ()) with
  | {|\q|} -> ()
  | line ->
    (try
       match line with
       | "" -> ()
       | {|\dt|} ->
         let tables = Catalog.get_table_names catalog in
         List.iter (fun name -> printf "%s\n" name) tables
       | _ when String.length line > 3 && String.sub line 0 3 = {|\d |} ->
         let name = String.sub line 3 (String.length line - 3) |> String.trim in
         let schema = Catalog.get_table_schema catalog name in
         printf "%s\n" (Metadata.Table_schema.show schema)
       | sql_query ->
         (match Catalog.execute_sql sql_query catalog tid with
          | Catalog.Stream s ->
            Seq.iter (fun tuple -> printf "%s\n" (Core.Tuple.show tuple)) s
          | Catalog.Nothing -> ())
     with
     | Core.Error.DBError e ->
       (match e with
        | Core.Error.Parser_error { line; col; tok } ->
          printf "Parse error at (%d,%d): unexpected token '%s'\n" line col tok
        | Core.Error.Unbound_alias_name _ -> printf "unbound alias name"
        | Core.Error.Duplicate_alias _ -> printf "duplicate alias"
        | Core.Error.Duplicate_column -> printf "duplicate column"
        | Core.Error.Unknown_column _ -> printf "unknown column"
        | Core.Error.Ambiguous_column _ -> printf "ambiguous column"
        | Core.Error.Invalid_primary_key -> printf "invalid primary key"
        | Core.Error.Table_not_found -> printf "Table not found\n"
        | Core.Error.Table_already_exists -> printf "Table already exists\n"
        | Core.Error.Type_mismatch -> printf "Type mismatch\n"
        | Core.Error.Buffer_pool_overflow -> printf "Buffer pool overflow"
        | Core.Error.Deadlock_victim -> printf "Deadlock victim\n")
     | e -> printf "Error: %s\n" (Printexc.to_string e));
    loop tid
;;

let () =
  let tid = Catalog.begin_new_transaction catalog in
  loop tid;
  Catalog.commit_transaction catalog tid
;;
