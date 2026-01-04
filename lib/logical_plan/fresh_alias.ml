let next_id = Atomic.make 0
let gen () = Printf.sprintf "0fresh_alias_%d" (Atomic.fetch_and_add next_id 1)
