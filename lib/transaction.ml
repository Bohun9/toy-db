type tran_id = TranId of int

let id = ref 0

let fresh_tid () =
  incr id;
  TranId !id
;;

module TranSet = Set.Make (struct
    type t = tran_id

    let compare = compare
  end)

module TranGraph = Graph.Imperative.Digraph.Abstract (struct
    type t = tran_id
  end)
