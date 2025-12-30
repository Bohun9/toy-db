type t

val empty : t
val extend : t -> string -> Metadata.Table_schema.t -> t
val merge : t -> t -> t
val resolve_field : t -> Core.Syntax.field_name -> Table_field.t
val alias_names : t -> string list
val fields : t -> Field.t list
