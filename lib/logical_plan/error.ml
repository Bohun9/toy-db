module C = Core

let type_mismatch = C.Error.semantic_error "type mismatch"
let table_not_found = C.Error.semantic_error "table not found"
let aggregate_without_grouping = C.Error.semantic_error "aggregate without grouping"
let unknown_field = C.Error.semantic_error "unknown field"
let ambiguous_field = C.Error.semantic_error "ambiguous field"

let duplicate_alias alias =
  C.Error.semantic_error (Printf.sprintf {|duplicate table alias "%s"|} alias)
;;
