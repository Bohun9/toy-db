module StringSet = Set.Make (String)

type t =
  { aliases : StringSet.t
  ; field_env : unit Table_field.Env.t
  }

let empty = { aliases = StringSet.empty; field_env = Table_field.Env.empty }

let extend_aliases alias aliases =
  if StringSet.mem alias aliases
  then raise @@ Error.duplicate_alias alias
  else StringSet.add alias aliases
;;

let extend { aliases; field_env } alias fields =
  { aliases = extend_aliases alias aliases
  ; field_env =
      List.fold_left (fun acc f -> Table_field.Env.extend acc f ()) field_env fields
  }
;;

let merge
      { aliases = aliases1; field_env = field_env1 }
      { aliases = aliases2; field_env = field_env2 }
  =
  { aliases = StringSet.fold extend_aliases aliases1 aliases2
  ; field_env = Table_field.Env.merge field_env1 field_env2
  }
;;

let resolve_field { field_env; _ } field_name =
  match Table_field.Env.resolve_field field_env field_name with
  | Ok (f, ()) -> f
  | Error e -> raise e
;;

let aliases { aliases; _ } = StringSet.to_list aliases
let fields { field_env; _ } = Table_field.Env.fields field_env
