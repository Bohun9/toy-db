module type DBFILE = sig
  type t

  val desc : t -> Tuple.tuple_descriptor
  val insert_tuple : t -> Tuple.t -> Transaction_id.t -> unit
  val scan_file : t -> Transaction_id.t -> Tuple.t Seq.t
end
