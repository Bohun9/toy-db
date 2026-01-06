type t

val create : unit -> t
val begin_transaction : t -> Core.Transaction_id.t -> unit
val locked_pages_list : t -> Core.Transaction_id.t -> Page_key.t list

val acquire_lock
  :  t
  -> Page_key.t
  -> Core.Transaction_id.t
  -> Perm.t
  -> (unit -> unit)
  -> unit

val release_locks : t -> Core.Transaction_id.t -> unit
val unsafe_release_lock : t -> Core.Transaction_id.t -> Page_key.t -> unit
val protect : t -> (unit -> 'a) -> 'a
