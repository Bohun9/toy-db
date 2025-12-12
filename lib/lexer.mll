{
open Parser

let kw_map = [
  "SELECT", SELECT;
  "FROM", FROM;
  "WHERE", WHERE;
  "JOIN", JOIN;
  "ON", ON;
  "CREATE", CREATE;
  "TABLE", TABLE;
  "INT", INT;
  "STRING", STRING;
  "INSERT", INSERT;
  "INTO", INTO;
  "VALUES", VALUES;
] |> List.to_seq |> Hashtbl.of_seq

let symbols_map = [
  "*", STAR;
  "=", EQ;
  ".", DOT;
  ",", COMMA;
  "(", LPAREN;
  ")", RPAREN;
] |> List.to_seq |> Hashtbl.of_seq

let make_id s = 
  try
    Hashtbl.find kw_map s
  with
    Not_found -> ID s

let make_symbol s = 
  Hashtbl.find symbols_map s
}

let digit = ['0'-'9']
let int = digit+
let id = ['a'-'z' 'A'-'Z'] ['a'-'z' 'A'-'Z' '_' '0'-'9']*
let symbol = ['*' '.' ',' '=' '(' ')'] | "<="

rule token = parse
  | [' ' '\t']             { token lexbuf }
  | ['\n']                 { Lexing.new_line lexbuf; token lexbuf }
  | int as i               { NUM (int_of_string i) }
  | id as id               { make_id id }
  | symbol as s            { make_symbol s }
  | eof                    { EOF }
  | _                      { failwith ("unexpected character: " ^ Lexing.lexeme lexbuf) }
