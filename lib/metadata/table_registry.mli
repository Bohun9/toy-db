type t

val create : int -> t
val add_table : t -> string -> Db_file.t -> unit
val delete_table : t -> string -> unit
val get_table : t -> string -> Db_file.t
val get_table_opt : t -> string -> Db_file.t option
val has_table : t -> string -> bool
val get_table_names : t -> string list
val get_tables : t -> (string * Db_file.t) list
