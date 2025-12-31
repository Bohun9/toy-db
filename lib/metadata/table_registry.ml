type t = { tables : (string, Db_file.t) Hashtbl.t }

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

let get_table reg name =
  match Hashtbl.find_opt reg.tables name with
  | Some f -> f
  | None -> raise Error.table_not_found
;;

let has_table reg name = Hashtbl.mem reg.tables name
let get_table_names reg = Hashtbl.to_seq_keys reg.tables |> List.of_seq
let get_tables reg = Hashtbl.to_seq reg.tables |> List.of_seq
