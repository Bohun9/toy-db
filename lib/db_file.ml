module type DBFILE = sig
  type t

  val get_desc : t -> Tuple.tuple_descriptor
  val insert_tuple : t -> Tuple.tuple -> unit
  val scan_file : t -> Tuple.tuple Seq.t
end
