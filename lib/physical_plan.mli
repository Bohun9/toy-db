type t

val build_plan : Table_registry.t -> Logical_plan.t -> t
val execute_plan : Transaction_id.t -> t -> Tuple.t Seq.t
