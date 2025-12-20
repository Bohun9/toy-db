type packed_dbfile =
  | PackedDBFile : (module Db_file.DBFILE with type t = 't) * 't -> packed_dbfile

type t = { tables : (string, packed_dbfile) Hashtbl.t }

let create n = { tables = Hashtbl.create n }

let add_table reg name m f =
  if Hashtbl.mem reg.tables name
  then raise Error.table_already_exists
  else Hashtbl.add reg.tables name (PackedDBFile (m, f))
;;

let delete_table reg name =
  if Hashtbl.mem reg.tables name
  then Hashtbl.remove reg.tables name
  else raise Error.table_not_found
;;

let get_table reg name =
  match Hashtbl.find_opt reg.tables name with
  | Some p -> p
  | None -> raise Error.table_not_found
;;

let has_table reg name = Hashtbl.mem reg.tables name
let get_table_names reg = Hashtbl.to_seq_keys reg.tables |> List.of_seq
