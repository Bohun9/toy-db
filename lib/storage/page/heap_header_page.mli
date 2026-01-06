type header_data
type t = header_data Generic_page.t

val create : int -> int -> t
val num_data_pages : t -> int
val set_num_data_pages : t -> int -> unit
val serialize : t -> bytes
val deserialize : int -> bytes -> t
