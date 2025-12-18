type t

val create : int -> Lock_manager.lock_manager -> t
val flush_all_pages : t -> unit
val begin_transaction : t -> Transaction.tran_id -> unit
val commit_transaction : t -> Transaction.tran_id -> unit

val get_page
  :  t
  -> Db_page.page_key
  -> Transaction.tran_id
  -> Lock_manager.permission
  -> (unit -> Db_page.db_page)
  -> (Db_page.db_page -> unit)
  -> Db_page.db_page
