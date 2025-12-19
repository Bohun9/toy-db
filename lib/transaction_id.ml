type t = TranId of int

let id = ref 0

let fresh_tid () =
  incr id;
  TranId !id
;;

let compare = compare
