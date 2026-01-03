open Toydb
module C = Core
module M = Metadata

let print_table ~headers ~rows =
  let cols = List.length headers in
  let col_widths =
    List.init cols (fun i ->
      let header_len = String.length (List.nth headers i) in
      let max_cell_len =
        rows
        |> List.map (fun row -> String.length (List.nth row i))
        |> List.fold_left max 0
      in
      max header_len max_cell_len)
  in
  let print_row row =
    List.iteri
      (fun i cell ->
         let w = List.nth col_widths i in
         Printf.printf " %-*s |" w cell)
      row;
    print_newline ()
  in
  let print_divider () =
    List.iter
      (fun w ->
         Printf.printf "%s" (String.make (w + 2) '-');
         print_string "+")
      col_widths;
    print_newline ()
  in
  print_row headers;
  print_divider ();
  List.iter print_row rows;
  Printf.printf "(%d rows)\n" (List.length rows)
;;

let print_rows ({ desc; rows } : Catalog.rows_info) =
  let headers = List.map Attribute.show desc in
  let data =
    Seq.map (fun tup -> C.Tuple.attributes tup |> List.map C.Value.show) rows
    |> List.of_seq
  in
  print_table ~headers ~rows:data
;;

let print_schema (sch : M.Table_schema.t) =
  let headers = [ "column"; "type" ] in
  let rows =
    M.Table_schema.columns sch
    |> List.map (fun ({ name; typ } : C.Syntax.column_data) -> [ name; C.Type.show typ ])
  in
  print_table ~headers ~rows
;;

let print_tables table_names =
  let headers = [ "table" ] in
  let rows = List.map (fun t -> [ t ]) table_names in
  print_table ~headers ~rows
;;
