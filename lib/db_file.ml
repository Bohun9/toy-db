module type DBFILE = sig
  type t

  val get_desc : t -> Tuple.tuple_descriptor
  val insert_tuple : t -> Tuple.tuple -> Transaction_id.t -> unit
  val scan_file : t -> Transaction_id.t -> Tuple.tuple Seq.t
end
