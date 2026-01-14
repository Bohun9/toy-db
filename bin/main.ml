open Toydb
module C = Core

let exec_line catalog line tid =
  match line with
  | "" | "BEGIN" -> tid
  | "COMMIT" ->
    Catalog.commit_transaction catalog tid;
    Catalog.begin_new_transaction catalog
  | "ABORT" ->
    Catalog.abort_transaction catalog tid;
    Catalog.begin_new_transaction catalog
  | {|\dt|} ->
    Catalog.get_table_names catalog |> Pretty_print.print_tables;
    tid
  | _ when String.length line > 3 && String.sub line 0 3 = {|\d |} ->
    let tname = String.sub line 3 (String.length line - 3) |> String.trim in
    Catalog.get_table_schema catalog tname |> Pretty_print.print_schema;
    tid
  | sql_query ->
    (match Catalog.execute_sql sql_query catalog tid with
     | Catalog.Rows rows -> Pretty_print.print_rows rows
     | Catalog.NoResult -> ());
    tid
;;

let print_error = function
  | C.Error.Lexer_error ch -> Printf.printf "lexer error: unexpected character '%c'\n" ch
  | C.Error.Parser_error { line; col; tok } ->
    Printf.printf "parse error at (%d,%d): unexpected token '%s'\n" line col tok
  | C.Error.Semantic_error msg -> Printf.printf "semantic error: %s\n" msg
  | C.Error.Type_error msg -> Printf.printf "type error: %s\n" msg
  | C.Error.Buffer_pool_overflow -> print_endline "buffer pool overflow error"
  | C.Error.Deadlock_victim -> print_endline "deadlock victim error"
;;

let next_tid_after_error catalog e tid =
  match e with
  | C.Error.Deadlock_victim -> Catalog.begin_new_transaction catalog
  | _ -> tid
;;

let rec loop catalog tid =
  Printf.printf "=> ";
  flush stdout;
  match String.trim (read_line ()) with
  | {|\q|} -> Catalog.commit_transaction catalog tid
  | line ->
    let next_tid =
      try exec_line catalog line tid with
      | C.Error.DBError e ->
        print_error e;
        next_tid_after_error catalog e tid
    in
    loop catalog next_tid
;;

let () =
  let db_dir = if Array.length Sys.argv > 1 then Sys.argv.(1) else "." in
  let catalog = Catalog.create db_dir 128 in
  let tid = Catalog.begin_new_transaction catalog in
  loop catalog tid
;;
