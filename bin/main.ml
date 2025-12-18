open Toydb
open Printf

let cat = Api.create_catalog "." 128

let rec loop tid =
  printf "=> ";
  flush stdout;
  match String.trim (read_line ()) with
  | {|\q|} -> ()
  | line ->
    (match line with
     | "" -> ()
     | {|\dt|} ->
       let tables = Catalog.get_table_names cat in
       List.iter (fun name -> printf "%s\n" name) tables
     | _ when String.length line > 3 && String.sub line 0 3 = {|\d |} ->
       let name = String.sub line 3 (String.length line - 3) |> String.trim in
       (try
          let desc = Catalog.get_table_desc cat name in
          printf "%s\n" (Tuple.show_tuple_descriptor desc)
        with
        | e -> printf "Error: %s\n" (Printexc.to_string e))
     | sql_query ->
       (try
          match Api.execute_sql sql_query cat tid with
          | Api.Stream s ->
            Seq.iter (fun tuple -> printf "%s\n" (Tuple.show_tuple tuple)) s
          | Api.Nothing -> ()
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
        | e -> printf "Error: %s\n" (Printexc.to_string e)));
    loop tid
;;

let () =
  printf "Welcome to toy-db.\n";
  let tid = Catalog.begin_new_transaction cat in
  loop tid;
  Catalog.commit_transaction cat tid
;;
(* Catalog.sync_to_disk cat *)
