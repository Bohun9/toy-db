type t

val create : int -> Lock_manager.t -> t
val begin_transaction : t -> Core.Transaction_id.t -> unit
val commit_transaction : t -> Core.Transaction_id.t -> unit

val get_page
  :  t
  -> Page_key.t
  -> Core.Transaction_id.t
  -> Perm.t
  -> (unit -> Page.Db_page.t)
  -> (Page.Db_page.t -> unit)
  -> Page.Db_page.t

val discard_file_pages : t -> string -> unit
val unsafe_release_lock : t -> Page_key.t -> Core.Transaction_id.t -> unit
