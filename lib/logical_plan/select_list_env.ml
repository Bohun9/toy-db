type t = unit Field.Env.t

let of_fields fields =
  List.fold_left (fun acc f -> Field.Env.extend acc f ()) Field.Env.empty fields
;;

let resolve_field env field_name =
  match Field.Env.resolve_field env field_name with
  | Ok (f, ()) -> f
  | Error e -> raise e
;;
