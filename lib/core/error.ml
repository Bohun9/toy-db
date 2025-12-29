type db_error =
  | Parser_error of
      { line : int
      ; col : int
      ; tok : string
      }
  | Aggregate_without_grouping
  | Unbound_alias_name of { alias : string }
  | Duplicate_alias of { alias : string }
  | Duplicate_column
  | Unknown_column of { column : string }
  | Ambiguous_column of { column : string }
  | Invalid_primary_key
  | Table_not_found
  | Table_already_exists
  | Type_mismatch
  | Deadlock_victim
  | Buffer_pool_overflow

exception DBError of db_error

let parser_error line col tok = DBError (Parser_error { line; col; tok })
let aggregate_without_grouping = DBError Aggregate_without_grouping
let unbound_alias_name alias = DBError (Unbound_alias_name { alias })
let duplicate_alias alias = DBError (Duplicate_alias { alias })
let duplicate_column = DBError Duplicate_column
let unknown_column column = DBError (Unknown_column { column })
let ambiguous_column column = DBError (Ambiguous_column { column })
let invalid_primary_key = DBError Invalid_primary_key
let table_not_found = DBError Table_not_found
let table_already_exists = DBError Table_already_exists
let type_mismatch = DBError Type_mismatch
let deadlock_victim = DBError Deadlock_victim
