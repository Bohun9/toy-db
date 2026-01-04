open Toydb

let exec_line catalog line =
  match line with
  | "" -> ()
  | {|\dt|} -> Catalog.get_table_names catalog |> Pretty_print.print_tables
  | _ when String.length line > 3 && String.sub line 0 3 = {|\d |} ->
    let tname = String.sub line 3 (String.length line - 3) |> String.trim in
    Catalog.get_table_schema catalog tname |> Pretty_print.print_schema
  | sql_query ->
    Catalog.with_tid catalog (fun tid ->
      match Catalog.execute_sql sql_query catalog tid with
      | Catalog.Rows rows -> Pretty_print.print_rows rows
      | Catalog.NoResult -> ())
;;

let print_error = function
  | Core.Error.Lexer_error ch ->
    Printf.printf "lexer error: unexpected character '%c'\n" ch
  | Core.Error.Parser_error { line; col; tok } ->
    Printf.printf "parse error at (%d,%d): unexpected token '%s'\n" line col tok
  | Core.Error.Semantic_error msg -> Printf.printf "semantic error: %s\n" msg
  | Core.Error.Type_error msg -> Printf.printf "type error: %s\n" msg
  | Core.Error.Buffer_pool_overflow -> print_endline "buffer pool overflow error"
  | Core.Error.Deadlock_victim -> print_endline "deadlock victim error"
;;

let rec loop catalog =
  Printf.printf "=> ";
  flush stdout;
  match String.trim (read_line ()) with
  | {|\q|} -> ()
  | line ->
    (try exec_line catalog line with
     | Core.Error.DBError e -> print_error e
     | e -> Printf.printf "Error: %s\n" (Printexc.to_string e));
    loop catalog
;;

let () =
  let catalog = Catalog.create "." 128 in
  loop catalog
;;
