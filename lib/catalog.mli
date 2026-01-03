type t

type rows_info =
  { desc : Physical_plan.Tuple_desc.t
  ; rows : Core.Tuple.t Seq.t
  }

type query_result =
  | Rows of rows_info
  | NoResult

val create : string -> int -> t
val execute_sql : string -> t -> Core.Transaction_id.t -> query_result
val get_table_names : t -> string list
val get_table_schema : t -> string -> Metadata.Table_schema.t
val begin_new_transaction : t -> Core.Transaction_id.t
val commit_transaction : t -> Core.Transaction_id.t -> unit
val with_tid : t -> (Core.Transaction_id.t -> 'a) -> 'a
val delete_db_files : t -> unit
