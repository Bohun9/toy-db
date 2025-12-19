open Toydb
open Printf

let lock_manager = Lock_manager.create ()
let buf_pool = Buffer_pool.create 128 lock_manager
let catalog = Catalog.create "." buf_pool

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
         let desc = Catalog.get_table_desc catalog name in
         printf "%s\n" (Tuple.show_tuple_descriptor desc)
       | sql_query ->
         (match Catalog.execute_sql sql_query catalog tid with
          | Catalog.Stream s ->
            Seq.iter (fun tuple -> printf "%s\n" (Tuple.show_tuple tuple)) s
          | Catalog.Nothing -> ())
     with
     | Error.DBError e ->
       (match e with
        | Error.Parser_error { line; col; tok } ->
          printf "Parse error at (%d,%d): unexpected token '%s'\n" line col tok
        | Error.Table_not_found -> printf "Table not found\n"
        | Error.Table_already_exists -> printf "Table already exists\n"
        | Error.Type_mismatch -> printf "Type mismatch\n"
        | Error.Buffer_pool_overflow -> printf "Buffer pool overflow"
        | Error.Deadlock_victim -> printf "Deadlock victim\n")
     | e -> printf "Error: %s\n" (Printexc.to_string e));
    loop tid
;;

let () =
  let tid = Catalog.begin_new_transaction catalog in
  loop tid;
  Catalog.commit_transaction catalog tid
;;
