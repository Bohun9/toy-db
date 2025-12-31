module C = Core

module type TABLE_FILE = sig
  type t

  val file_path : t -> string
  val insert_tuple : t -> C.Tuple.t -> C.Transaction_id.t -> unit
  val scan_file : t -> C.Transaction_id.t -> C.Tuple.t Seq.t
  val schema : t -> Table_schema.t
end

module type INDEX_FILE = sig
  include TABLE_FILE

  val key_info : t -> Table_schema.column_data
  val range_scan : t -> C.Value_interval.t -> C.Transaction_id.t -> C.Tuple.t Seq.t
end

type table_file = PackedTable : (module TABLE_FILE with type t = 'a) * 'a -> table_file
type index_file = PackedIndex : (module INDEX_FILE with type t = 'a) * 'a -> index_file

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
