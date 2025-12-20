type t = TranId of int [@@deriving show { with_path = false }]

let id = Atomic.make 0

let fresh_tid () =
  let n = Atomic.fetch_and_add id 1 in
  TranId n
;;

let hash = Hashtbl.hash
let compare = compare
let equal = ( = )
