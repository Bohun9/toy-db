type leaf_data
type t = leaf_data Generic_page.t
type tuples_info

val leaf_id : char
val max_num_tuples_for_schema : Metadata.Table_schema.t -> int

val create
  :  int
  -> Metadata.Table_schema.t
  -> int
  -> tuples_info option
  -> int option
  -> t

val next_leaf : t -> int option
val set_next_leaf : t -> int option -> unit
val lowest_key : t -> Core.Value.t
val serialize : t -> bytes
val deserialize : int -> Metadata.Table_schema.t -> int -> bytes -> t
val safe : t -> Btree_op.t -> bool
val can_coalesce : t -> t -> bool
val coalesce : t -> t -> unit
val scan_page : t -> Core.Tuple.t Seq.t

type insert_result =
  | Inserted
  | Split of tuples_info

val insert_tuple : t -> Core.Tuple.t -> insert_result
val insert_tuple_no_split : t -> Core.Tuple.t -> unit

type delete_result =
  | Deleted
  | Underfull

val delete_tuple : t -> Core.Tuple.t -> bool -> delete_result
val delete_highest_tuple : t -> Core.Tuple.t
val delete_lowest_tuple : t -> Core.Tuple.t
