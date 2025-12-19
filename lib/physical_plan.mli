type physical_plan

val make_plan : Table_registry.t -> Syntax.dml -> physical_plan
val execute_plan : Transaction_id.t -> physical_plan -> Tuple.tuple Seq.t
