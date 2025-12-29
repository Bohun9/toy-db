{
open Parser

let kw_map = [
  "SELECT", SELECT;
  "FROM", FROM;
  "WHERE", WHERE;
  "AND", AND;
  "JOIN", JOIN;
  "ON", ON;
  "CREATE", CREATE;
  "TABLE", TABLE;
  "INT", INT;
  "STRING", STRING;
  "INSERT", INSERT;
  "INTO", INTO;
  "VALUES", VALUES;
  "PRIMARY", PRIMARY;
  "KEY", KEY;
  "GROUP", GROUP;
  "BY", BY;
  "COUNT", COUNT;
  "SUM", SUM;
  "AVG", AVG;
  "MIN", MIN;
  "MAX", MAX;
  "AS", AS;
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
let id = ['a'-'z' 'A'-'Z' '_'] ['a'-'z' 'A'-'Z' '_' '0'-'9']*
let symbol = ['*' '.' ',' '=' '(' ')'] | "<="
let string = '"' [^ '"' '\n']* '"'

rule token = parse
  | [' ' '\t']             { token lexbuf }
  | ['\n']                 { Lexing.new_line lexbuf; token lexbuf }
  | string as s            { STRING_LIT (String.sub s 1 (String.length s - 2)) }
  | int as i               { INT_LIT (int_of_string i) }
  | id as id               { make_id id }
  | symbol as s            { make_symbol s }
  | eof                    { EOF }
  | _                      { failwith ("unexpected character: " ^ Lexing.lexeme lexbuf) }
