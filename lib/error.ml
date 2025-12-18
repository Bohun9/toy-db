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
