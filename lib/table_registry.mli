type packed_dbfile =
  | PackedDBFile : (module Db_file.DBFILE with type t = 'a) * 'a -> packed_dbfile

type t

val create : int -> t
val add_table : t -> string -> (module Db_file.DBFILE with type t = 'a) -> 'a -> unit
val delete_table : t -> string -> unit
val get_table : t -> string -> packed_dbfile
val has_table : t -> string -> bool
val get_table_names : t -> string list
