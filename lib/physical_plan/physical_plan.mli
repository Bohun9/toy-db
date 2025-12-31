type t

val build_plan : Metadata.Table_registry.t -> Logical_plan.t -> t
val execute_plan : Core.Transaction_id.t -> t -> Core.Tuple.t Seq.t
