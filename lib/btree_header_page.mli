type header_data
type t = header_data Generic_page.t

val create : int -> t
val root : t -> int
val set_root : t -> int -> unit
val serialize : t -> bytes
val deserialize : bytes -> t
