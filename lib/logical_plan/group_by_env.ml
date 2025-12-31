type t = int Table_field.Env.t

(* GROUP BY f, f is allowed, so the reference to f should not be an error.
   We can just remove duplicates to achieve that, because the same resolved fields
   refer to the same column (otherwise the resolution would fail). *)

let of_fields fields =
  let ifields = List.mapi (fun i f -> i, f) fields in
  let compare (_, f1) (_, f2) = compare f1 f2 in
  let unique_ifields = List.sort_uniq compare ifields in
  List.fold_left
    (fun acc (i, f) -> Table_field.Env.extend acc f i)
    Table_field.Env.empty
    unique_ifields
;;

let resolve_field env field_name =
  match Table_field.Env.resolve_field env field_name with
  | Ok r -> r
  | Error e -> raise e
;;
