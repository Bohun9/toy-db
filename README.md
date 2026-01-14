# ToyDB

ToyDB is an educational relational database management system implemented in OCaml.

## Features

- SQL support
- B+ tree index with latch crabbing protocol
- Concurrent transactions using strong strict 2-phase locking with deadlock detection
- Metadata about tables is stored as internal tables; DDL queries are internally executed as DML queries

## SQL Support

#### Data Types
- `INT` (8 bytes)
- `STRING` (max 32 bytes)

#### Data Definition
- `CREATE TABLE table (...)`  
- `DROP TABLE table`  

#### Data Modification
- `INSERT INTO table VALUES (...)`
- `DELETE FROM table WHERE ...`
- `COMMIT` and `ABORT`

#### Data Query
- `SELECT ... FROM ...`  
- `WHERE` (predicates of the form `field op constant`)
- `JOIN`
- `GROUP BY`
- `LIMIT` and `OFFSET`
- subqueries

## Running the Shell

```bash
opam install . --deps-only
dune exec toydb -- <db_dir>
```

## Demo
<img src="docs/demo.png" alt="ToyDB demo" width="800">

## Missing Features
- Server architecture
- Multi-version concurrency control
- Crash recovery and logging system
- Query optimization
