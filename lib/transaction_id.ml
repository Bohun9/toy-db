type t = TranId of int [@@deriving show { with_path = false }]

let next_tid = Atomic.make 0

let fresh_tid () =
  let tid = Atomic.fetch_and_add next_tid 1 in
  TranId tid
;;

let hash = Hashtbl.hash
let compare = compare
let equal = ( = )
