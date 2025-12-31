module C = Core

type t =
  { table_alias : string option (* [None] for virtual columns. *)
  ; column : string
  ; typ : C.Type.t
  }

let virtual_field column typ = { table_alias = None; column; typ }

let field_name_match fname field =
  match fname with
  | C.Syntax.UnqualifiedField { column } -> column = field.column
  | C.Syntax.QualifiedField { alias; column } ->
    Some alias = field.table_alias && column = field.column
;;

type field = t

module type ENV = sig
  type 'a t

  val empty : 'a t
  val extend : 'a t -> field -> 'a -> 'a t
  val merge : 'a t -> 'a t -> 'a t
  val resolve_field : 'a t -> C.Syntax.field_name -> (field * 'a, exn) result
  val fields : 'a t -> field list
end

module Env : ENV = struct
  type 'a t = (field * 'a) list

  let empty = []
  let extend env f data = (f, data) :: env
  let merge env1 env2 = env2 @ env1

  let resolve_field env field_name =
    let matches = List.filter (fun (f, _) -> field_name_match field_name f) env in
    match matches with
    | [] -> Error Error.unknown_field
    | [ match' ] -> Ok match'
    | _ -> Error Error.ambiguous_field
  ;;

  let fields env = env |> List.rev |> List.map fst
end
