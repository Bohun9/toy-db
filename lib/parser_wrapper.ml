let parse_sql s =
  let lexbuf = Lexing.from_string s in
  try Parser.sql Lexer.token lexbuf with
  | Parser.Error ->
    let pos = lexbuf.lex_curr_p in
    let line = pos.pos_lnum in
    let col = pos.pos_cnum - pos.pos_bol in
    let tok = Lexing.lexeme lexbuf in
    raise (Error.DBError (Error.Parser_error { line; col; tok }))
;;
