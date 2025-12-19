type packed_dbfile =
  | PackedDBFile : (module Db_file.DBFILE with type t = 't) * 't -> packed_dbfile

type t = TableRegistry of (string, packed_dbfile) Hashtbl.t

let create n = TableRegistry (Hashtbl.create n)

let add_table (TableRegistry tables) name m f =
  if Hashtbl.mem tables name
  then raise Error.table_already_exists
  else Hashtbl.add tables name (PackedDBFile (m, f))
;;

let delete_table (TableRegistry tables) name =
  if Hashtbl.mem tables name
  then Hashtbl.remove tables name
  else raise Error.table_not_found
;;

let get_table (TableRegistry tables) name =
  match Hashtbl.find_opt tables name with
  | Some p -> p
  | None -> raise Error.table_not_found
;;

let has_table (TableRegistry tables) name = Hashtbl.mem tables name
let get_table_names (TableRegistry tables) = Hashtbl.to_seq_keys tables |> List.of_seq
