type t

val create : string -> Metadata.Table_schema.t -> Buffer_pool.t -> t
val file_path : t -> string
val schema : t -> Metadata.Table_schema.t
val insert_tuple : t -> Core.Tuple.t -> Core.Transaction_id.t -> unit
val delete_tuple : t -> Core.Tuple.t -> Core.Transaction_id.t -> unit
val scan_file : t -> Core.Transaction_id.t -> Core.Tuple.t Seq.t
