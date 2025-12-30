module C = Core

let duplicate_column = C.Error.semantic_error "duplicate column"
let invalid_primary_key = C.Error.semantic_error "invalid primary key"
let table_not_found = C.Error.semantic_error "table not found"
let table_already_exists = C.Error.semantic_error "table already exists"
