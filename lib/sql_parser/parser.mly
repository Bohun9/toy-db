%{
open Core
open Syntax
%}

%type <sql> sql
%start sql

%token EOF
%token <string> ID
%token <int> INT_LIT
%token <string> STRING_LIT

%token SELECT FROM WHERE AND JOIN ON CREATE TABLE INSERT DELETE INTO VALUES PRIMARY KEY GROUP BY AS ORDER LIMIT OFFSET
%token COUNT SUM AVG MIN MAX
%token ASC DESC
%token INT STRING

%token EQ LE LT GE GT
%token STAR DOT COMMA LPAREN RPAREN
%%

relop
  : EQ { Eq }
  | LE { Le }
  | LT { Lt }
  | GE { Ge }
  | GT { Gt }

field
  : ID        { UnqualifiedField { column = $1 } }
  | ID DOT ID { QualifiedField { alias = $1; column = $3 } }

value 
  : INT_LIT    { VInt $1 }
  | STRING_LIT { VString $1 }

tuple
  : LPAREN separated_list(COMMA, value) RPAREN { $2 }

tuples
  : separated_list(COMMA, tuple) { $1 }

aggregate
  : COUNT { Count }
  | SUM   { Sum }
  | AVG   { Avg }
  | MAX   { Max }
  | MIN   { Min }

order
  : ASC  { Asc }
  | DESC { Desc }

alias
  : AS ID { $2 }

order_item
  : field order? { { field = $1; order = $2 } }

select_item
  : field                                { SelectField { field = $1 } }
  | aggregate LPAREN field RPAREN alias? { SelectAggregate { agg_kind = $1; field = $3; name = $5 } }

select_list
  : STAR                               { Star }
  | separated_list(COMMA, select_item) { SelectList $1 }

table_expr 
  : ID alias?                                    { Table { name = $1; alias = $2 } }
  | LPAREN select_stmt RPAREN alias?             { Subquery { select = $2; alias = $4 } }
  | table_expr JOIN table_expr ON field EQ field { Join { tab1 = $1; tab2 = $3; field1 = $5; field2 = $7 } }

predicate
  : field relop value { { field = $1; op = $2; value = $3 } }

where_clause
  : /* empty */                          { [] }
  | WHERE separated_list(AND, predicate) { $2 }

group_by_clause
  : GROUP BY separated_list(COMMA, field) { { group_by_fields = $3 } }

order_by_clause
  : ORDER BY separated_list(COMMA, order_item) { $3 }

limit_clause
  : LIMIT INT_LIT { $2 }

offset_clause
  : OFFSET INT_LIT { $2 }

select_stmt
  : SELECT select_list FROM table_expr where_clause group_by_clause? order_by_clause? limit_clause? offset_clause?
      { { select_list = $2; table_expr = $4; predicates = $5; group_by = $6; order_by = $7; limit = $8; offset = $9 } }

stmt
  : select_stmt { Select $1 }
  | INSERT INTO ID VALUES tuples { InsertValues { table = $3; tuples = $5 } }
  | DELETE FROM ID where_clause { Delete { table = $3; predicates = $4 } }

col_type
  : INT    { Type.Int }
  | STRING { Type.String }

col_data
  : ID col_type { { Syntax.name = $1; typ = $2 } }

table_primary_key
  : /* empty */                      { None }
  | DOT PRIMARY KEY LPAREN ID RPAREN { Some $5 } 

table_schema
  : separated_list(COMMA, col_data) table_primary_key { { Syntax.columns = $1; primary_key = $2 } }

ddl
  : CREATE TABLE ID LPAREN table_schema RPAREN { CreateTable($3, $5) }

sql
  : stmt EOF { SQL_Stmt $1 }
  | ddl EOF { SQL_DDL $1 }

%%
