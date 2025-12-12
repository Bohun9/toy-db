type t

val create : int -> t
val flush_all_pages : t -> unit

val get_page
  :  t
  -> Db_page.page_key
  -> (unit -> Db_page.db_page)
  -> (Db_page.db_page -> unit)
  -> Db_page.db_page
