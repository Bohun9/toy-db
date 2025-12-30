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

%token SELECT FROM WHERE AND JOIN ON CREATE TABLE INSERT INTO VALUES PRIMARY KEY GROUP BY AS ORDER LIMIT OFFSET
%token COUNT SUM AVG MIN MAX
%token ASC DESC
%token INT STRING

%token STAR EQ DOT COMMA LPAREN RPAREN
%%

relop
  : EQ { Eq }

field
  : ID        { PureFieldName $1 }
  | ID DOT ID { QualifiedFieldName { alias = $1; column = $3 } }

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

order_item
  : field order? { { field = $1; order = $2 } }

select_item
  : field                               { SelectField { field = $1 } }
  | aggregate LPAREN field RPAREN AS ID { SelectAggregate { agg_kind = $1; field = $3; name = $6 } }

select_list
  : STAR                               { Star }
  | separated_list(COMMA, select_item) { SelectList $1 }

table_expr 
  : ID                                           { Table { name = $1; alias = None } }
  | table_expr JOIN table_expr ON field EQ field { Join { tab1 = $1; tab2 = $3; field1 = $5; field2 = $7 } }

predicate
  : field relop value { { field = $1; op = $2; value = $3 } }

where_clause
  : /* empty */                          { [] }
  | WHERE separated_list(AND, predicate) { $2 }

group_by_clause
  : /* empty */                           { None }
  | GROUP BY separated_list(COMMA, field) { Some { group_by_fields = $3 } }

order_by_clause
  : /* empty */                                { None }
  | ORDER BY separated_list(COMMA, order_item) { Some $3 }

limit_clause
  : LIMIT INT_LIT { $2 }

offset_clause
  : OFFSET INT_LIT { $2 }

stmt
  : SELECT select_list FROM table_expr where_clause group_by_clause order_by_clause limit_clause? offset_clause?
      { Select { select_list = $2; table_expr = $4; predicates = $5; group_by = $6; order_by = $7; limit = $8; offset = $9 } }
  | INSERT INTO ID VALUES tuples { InsertValues { table = $3; tuples = $5 } }

col_type
  : INT    { Type.TInt }
  | STRING { Type.TString }

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
