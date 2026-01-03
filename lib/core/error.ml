type db_error =
  | Lexer_error of char
  | Parser_error of
      { line : int
      ; col : int
      ; tok : string
      }
  | Semantic_error of string
  | Type_error of string
  | Deadlock_victim
  | Buffer_pool_overflow

exception DBError of db_error

let lexer_error ch = DBError (Lexer_error ch)
let parser_error ~line ~col ~tok = DBError (Parser_error { line; col; tok })
let semantic_error msg = DBError (Semantic_error msg)
let type_error msg = DBError (Type_error msg)
let deadlock_victim = DBError Deadlock_victim
let buffer_pool_overflow = DBError Buffer_pool_overflow
