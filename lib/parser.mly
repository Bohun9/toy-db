%{
open Syntax
%}

%type <sql> sql
%start sql

%token EOF
%token <string> ID
%token <int> NUM

%token SELECT FROM WHERE JOIN ON CREATE TABLE INSERT INTO VALUES
%token INT STRING

%token STAR EQ DOT COMMA LPAREN RPAREN
%%

binop
  : EQ { Eq }

field_name
  : ID        { PureFieldName $1 }
  | ID DOT ID { QualifiedFieldName { alias = $1; column = $3 } }

value 
  : NUM { VInt $1 }

tuple
  : LPAREN separated_list(COMMA, value) RPAREN { $2 }

tuples
  : separated_list(COMMA, tuple) { $1 }

expr 
  : value           { EValue $1 }
  | expr binop expr { EBinop($1, $2, $3) }
  | field_name      { EField $1 }

select_exprs
  : STAR { Star }

table_expr 
  : ID { Table { name = $1; alias = None } }
  | table_expr JOIN table_expr ON expr EQ expr { Join { tab1 = $1; tab2 = $3; e1 = $5; e2 = $7 } }

where_clause
  : /* empty */ { [] }
  | WHERE expr  { [$2] }

dml
  : SELECT select_exprs FROM table_expr where_clause { Select { exprs = $2; table_expr = $4; predicates = $5 } }
  | INSERT INTO ID VALUES tuples                     { InsertValues { table = $3; tuples = $5 } }

col_type
  : INT    { TInt }
  | STRING { TString }

col_data
  : ID col_type { ColumnData { name = $1; typ = $2 } }

table_schema
  : separated_list(COMMA, col_data) { $1 }

ddl
  : CREATE TABLE ID LPAREN table_schema RPAREN { CreateTable($3, $5) }

sql
  : dml EOF { SQL_DML $1 }
  | ddl EOF { SQL_DDL $1 }

%%
