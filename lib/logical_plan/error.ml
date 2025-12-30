module C = Core

let type_mismatch = C.Error.semantic_error "type mismatch"
let table_not_found = C.Error.semantic_error "table not found"
let aggregate_without_grouping = C.Error.semantic_error "aggregate without grouping"
