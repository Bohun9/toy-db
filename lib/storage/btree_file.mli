type t

val create : string -> Metadata.Table_schema.t -> Buffer_pool.t -> int -> t
val file_path : t -> string
val schema : t -> Metadata.Table_schema.t
val key_info : t -> Metadata.Table_schema.column_data
val insert_tuple : t -> Core.Tuple.t -> Core.Transaction_id.t -> unit
val delete_tuple : t -> Core.Tuple.t -> Core.Transaction_id.t -> unit
val scan_file : t -> Core.Transaction_id.t -> Core.Tuple.t Seq.t
val range_scan : t -> Core.Value_interval.t -> Core.Transaction_id.t -> Core.Tuple.t Seq.t
