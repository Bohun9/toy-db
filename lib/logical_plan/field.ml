type t =
  { table_alias : string option (* [None] for virtual columns. *)
  ; column : string
  ; typ : Core.Type.t
  }
