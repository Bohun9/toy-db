type packed_dbfile =
  | PackedDBFile : (module Db_file.DBFILE with type t = 't) * 't -> packed_dbfile

type t

val create : string -> Buffer_pool.t -> t
val add_table : t -> string -> Tuple.tuple_descriptor -> clear:bool -> unit
val get_table : t -> string -> packed_dbfile
val process_ddl_query : t -> Syntax.ddl -> unit
val get_table_names : t -> string list
val get_table_desc : t -> string -> Tuple.tuple_descriptor
val begin_new_transaction : t -> Transaction.tran_id
val commit_transaction : t -> Transaction.tran_id -> unit
val sync_to_disk : t -> unit
