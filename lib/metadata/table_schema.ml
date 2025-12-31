module C = Core

type column_data = C.Syntax.column_data [@@deriving show]

type t =
  { columns : column_data list
  ; primary_key : string option
  }
[@@deriving show]

let find_column columns cname =
  List.find_opt (fun (c : C.Syntax.column_data) -> c.name = cname) columns
;;

let create columns primary_key =
  let check_name_uniqueness () =
    let names = List.map (fun (c : C.Syntax.column_data) -> c.name) columns in
    if List.length (List.sort_uniq String.compare names) = List.length names
    then Ok ()
    else Error Error.duplicate_column
  in
  let check_primary_key key_column =
    match find_column columns key_column with
    | Some _ -> Ok ()
    | None -> Error Error.invalid_primary_key
  in
  let ( let* ) = Result.bind in
  let* () = check_name_uniqueness () in
  let* () =
    match primary_key with
    | None -> Ok ()
    | Some key_column -> check_primary_key key_column
  in
  Ok { columns; primary_key }
;;

let columns sch = sch.columns

let primary_key sch =
  Option.map
    (fun pk ->
       ( pk
       , List.find_index (fun (c : C.Syntax.column_data) -> c.name = pk) sch.columns
         |> Option.get ))
    sch.primary_key
;;

let types sch = List.map (fun (c : C.Syntax.column_data) -> c.typ) sch.columns
let column_type sch i = List.nth (types sch) i
let find_column sch cname = find_column sch.columns cname
let typecheck sch tuple_type = types sch = tuple_type
