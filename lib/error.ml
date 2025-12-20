type db_error =
  | Parser_error of
      { line : int
      ; col : int
      ; tok : string
      }
  | Table_not_found
  | Table_already_exists
  | Type_mismatch
  | Deadlock_victim
  | Buffer_pool_overflow

exception DBError of db_error

let parser_error line col tok = DBError (Parser_error { line; col; tok })
let table_not_found = DBError Table_not_found
let table_already_exists = DBError Table_already_exists
let type_mismatch = DBError Type_mismatch
let deadlock_victim = DBError Deadlock_victim
