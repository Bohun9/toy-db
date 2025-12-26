type column_data =
  { name : string
  ; typ : Type.t
  }
[@@deriving show]

type t = column_data list [@@deriving show]

let from_list xs = List.map (fun (name, typ) -> { name; typ }) xs
let types sch = List.map (fun { typ; _ } -> typ) sch
let column_type sch i = List.nth (types sch) i
let find_column sch cname = List.find_opt (fun cdata -> cdata.name = cname) sch
let typecheck sch tuple_type = types sch = tuple_type
