type t = { tables : (string, Packed_dbfile.t) Hashtbl.t }

let create n = { tables = Hashtbl.create n }

let add_table reg name packed =
  if Hashtbl.mem reg.tables name
  then raise Error.table_already_exists
  else Hashtbl.add reg.tables name packed
;;

let delete_table reg name =
  if Hashtbl.mem reg.tables name
  then Hashtbl.remove reg.tables name
  else raise Error.table_not_found
;;

let get_table_opt reg = Hashtbl.find_opt reg.tables
let get_table reg = Hashtbl.find reg.tables
let has_table reg name = Hashtbl.mem reg.tables name
let get_table_names reg = Hashtbl.to_seq_keys reg.tables |> List.of_seq
