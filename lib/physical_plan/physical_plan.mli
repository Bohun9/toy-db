module Tuple_desc = Tuple_desc
module Attribute = Attribute

type physical_plan_data

type t =
  { desc : Tuple_desc.t
  ; plan : physical_plan_data
  }

val build_plan : Metadata.Table_registry.t -> Logical_plan.t -> t
val execute_plan : Core.Transaction_id.t -> t -> Core.Tuple.t Seq.t
