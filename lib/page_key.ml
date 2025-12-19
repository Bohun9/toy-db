type t =
  | PageKey of
      { file : string
      ; page_no : int
      }

let compare = compare

(* module Set = Set.Make (struct *)
(*     type nonrec t = t *)
(**)
(*     let compare = compare *)
(*   end) *)
