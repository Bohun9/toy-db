type t

val create : bytes -> t
val read_int16_le : t -> int
val read_int64_le : t -> Int64.t
val read_char : t -> char
val read_string : t -> int -> string
