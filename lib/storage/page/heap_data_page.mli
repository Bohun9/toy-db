type page_data
type t = page_data Generic_page.t

val create : int -> Metadata.Table_schema.t -> t
val has_empty_slot : t -> bool
val serialize : t -> bytes
val deserialize : int -> Metadata.Table_schema.t -> bytes -> t
val insert_tuple : t -> Core.Tuple.t -> unit
val delete_tuple : t -> Core.Record_id.t -> unit
val scan_page : t -> Core.Tuple.t Seq.t
