open Core

type t

val create : int -> Lock_manager.t -> t
val flush_all_pages : t -> unit
val begin_transaction : t -> Transaction_id.t -> unit
val commit_transaction : t -> Transaction_id.t -> unit

val get_page
  :  t
  -> Page_key.t
  -> Transaction_id.t
  -> Lock_manager.permission
  -> (unit -> Db_page.t)
  -> (Db_page.t -> unit)
  -> Db_page.t
