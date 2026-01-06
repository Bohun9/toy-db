type internal_data
type t = internal_data Generic_page.t

val internal_id : char

type create_data =
  | RootData of
      { child1 : int
      ; key : Core.Value.t
      ; child2 : int
      }
  | InternalData of internal_data

val create : int -> Core.Type.t -> create_data -> t
val serialize : t -> bytes
val deserialize : int -> Core.Type.t -> bytes -> t
val safe : t -> Btree_op.t -> bool
val find_child : t -> Core.Value.t -> int
val can_coalesce : t -> t -> bool
val coalesce : t -> Core.Value.t -> t -> unit
val replace_key : t -> Core.Value.t -> Core.Value.t -> unit

type insert_result =
  | Inserted
  | Split of
      { sep_key : Core.Value.t
      ; internal_data : internal_data
      }

val insert_entry : t -> Core.Value.t -> int -> insert_result
val insert_entry_at_beginning : t -> int -> Core.Value.t -> unit
val insert_entry_at_end : t -> Core.Value.t -> int -> unit

type delete_result =
  | Deleted
  | EmptyRoot of { new_root : int }
  | Underfull

val delete_entry : t -> Core.Value.t -> int -> bool -> delete_result
val delete_highest_entry : t -> Core.Value.t * int
val delete_lowest_entry : t -> int * Core.Value.t

type sibling =
  { node : int
  ; sep_key : Core.Value.t
  ; left : bool
  }

val get_sibling : t -> int -> sibling
