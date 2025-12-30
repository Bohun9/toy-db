type t

val empty : t
val extend_base : t -> string -> Metadata.Table_schema.t -> t
val extend_derived : t -> string -> Table_field.t list -> t
val merge : t -> t -> t
val resolve_field : t -> Core.Syntax.field_name -> Table_field.t
val alias_names : t -> string list
val fields : t -> Table_field.t list
