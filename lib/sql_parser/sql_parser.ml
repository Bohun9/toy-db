open Core

let parse s =
  let lexbuf = Lexing.from_string s in
  try Parser.sql Lexer.token lexbuf with
  | Parser.Error ->
    let pos = lexbuf.lex_curr_p in
    let line = pos.pos_lnum in
    let col = pos.pos_cnum - pos.pos_bol in
    let tok = Lexing.lexeme lexbuf in
    raise (Error.parser_error line col tok)
;;
