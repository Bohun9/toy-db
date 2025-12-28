open Core
open Metadata

type t

type query_result =
  | Stream of Tuple.t Seq.t
  | Nothing

val create : string -> int -> t
val execute_sql : string -> t -> Transaction_id.t -> query_result

(* val add_table : t -> string -> Tuple.tuple_descriptor -> clear:bool -> unit *)
(* val get_table : t -> string -> packed_dbfile *)

(* val process_ddl_query : t -> Syntax.ddl -> unit *)
val get_table_names : t -> string list
val get_table_schema : t -> string -> Table_schema.t
val begin_new_transaction : t -> Transaction_id.t
val commit_transaction : t -> Transaction_id.t -> unit
val with_tid : t -> (Transaction_id.t -> 'a) -> 'a
val sync_to_disk : t -> unit
val delete_db_files : t -> unit
