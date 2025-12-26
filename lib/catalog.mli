(* type packed_dbfile = *)
(*   | PackedDBFile : (module Db_file.DBFILE with type t = 't) * 't -> packed_dbfile *)

type t

type query_result =
  | Stream of Tuple.t Seq.t
  | Nothing

val create : string -> Buffer_pool.t -> t
val execute_sql : string -> t -> Transaction_id.t -> query_result

(* val add_table : t -> string -> Tuple.tuple_descriptor -> clear:bool -> unit *)
(* val get_table : t -> string -> packed_dbfile *)

(* val process_ddl_query : t -> Syntax.ddl -> unit *)
val get_table_names : t -> string list
val get_table_schema : t -> string -> Table_schema.t
val begin_new_transaction : t -> Transaction_id.t
val commit_transaction : t -> Transaction_id.t -> unit
val sync_to_disk : t -> unit
