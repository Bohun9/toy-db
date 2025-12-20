type t

val make_plan : Table_registry.t -> Syntax.dml -> t
val execute_plan : Transaction_id.t -> t -> Tuple.t Seq.t
