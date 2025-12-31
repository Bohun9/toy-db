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
          | Catalog.Rows s ->
            Seq.iter (fun tuple -> printf "%s\n" (Core.Tuple.show tuple)) s
          | Catalog.NoResult -> ())
     with
     | Core.Error.DBError e ->
       (match e with
        | Core.Error.Parser_error { line; col; tok } ->
          printf "Parse error at (%d,%d): unexpected token '%s'\n" line col tok
        | Core.Error.Semantic_error msg -> printf "semantic error: %s\n" msg
        | Core.Error.Type_error msg -> printf "type error: %s\n" msg
        | Core.Error.Buffer_pool_overflow -> printf "buffer pool overflow error\n"
        | Core.Error.Deadlock_victim -> printf "deadlock victim error\n")
     | e -> printf "Error: %s\n" (Printexc.to_string e));
    loop tid
;;

let () =
  let tid = Catalog.begin_new_transaction catalog in
  loop tid;
  Catalog.commit_transaction catalog tid
;;
