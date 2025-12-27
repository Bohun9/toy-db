type t

val create : int -> t
val add_table : t -> string -> Packed_dbfile.t -> unit
val delete_table : t -> string -> unit
val get_table : t -> string -> Packed_dbfile.t
val get_table_opt : t -> string -> Packed_dbfile.t option
val has_table : t -> string -> bool
val get_table_names : t -> string list
