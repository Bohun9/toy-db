type table_file =
  | PackedTable : (module Db_file.TABLE_FILE with type t = 'a) * 'a -> table_file

type index_file =
  | PackedIndex : (module Db_file.INDEX_FILE with type t = 'a) * 'a -> index_file

type t =
  | TableFile of table_file
  | IndexFile of index_file

let to_table_file = function
  | TableFile f -> f
  | IndexFile (PackedIndex (m, f)) ->
    let module M = (val m) in
    PackedTable ((module M), f)
;;

let schema f =
  let (PackedTable (m, f)) = to_table_file f in
  let module M = (val m) in
  M.schema f
;;

let file_path f =
  let (PackedTable (m, f)) = to_table_file f in
  let module M = (val m) in
  M.file_path f
;;
