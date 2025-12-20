type t

val fresh_tid : unit -> t
val show : t -> string
val compare : t -> t -> int
val hash : t -> int
val equal : t -> t -> bool
