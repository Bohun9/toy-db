module type DBFILE = sig
  type t

  val get_desc : t -> Tuple.tuple_descriptor
  val insert_tuple : t -> Tuple.tuple -> Transaction.tran_id -> unit
  val scan_file : t -> Transaction.tran_id -> Tuple.tuple Seq.t
end
