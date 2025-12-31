type t

val empty : t
val extend : t -> string -> Table_field.t list -> t
val merge : t -> t -> t
val resolve_field : t -> Core.Syntax.field_name -> Table_field.t
val aliases : t -> string list
val fields : t -> Table_field.t list
