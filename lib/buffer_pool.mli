type t

val create : int -> Lock_manager.t -> t
val flush_all_pages : t -> unit
val begin_transaction : t -> Transaction_id.t -> unit
val commit_transaction : t -> Transaction_id.t -> unit
val with_tid : t -> (Transaction_id.t -> 'a) -> 'a

val get_page
  :  t
  -> Page_key.t
  -> Transaction_id.t
  -> Lock_manager.permission
  -> (unit -> Db_page.t)
  -> (Db_page.t -> unit)
  -> Db_page.t
