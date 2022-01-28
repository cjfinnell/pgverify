[![codecov](https://codecov.io/gh/udacity/pgverify/branch/main/graph/badge.svg?token=LsTntCpkUr)](https://codecov.io/gh/udacity/pgverify)

`pgverify` is a tool for verifying consistency of data between different database engines.

# Why?

Migrating database engines can be a huge headache; newer relational databases often try to mitigate this by asserting some level of PostgreSQL syntax compatability, but there can be small differences in data types, output formats, etc that make it difficult to affirmatively verify that the actual data between instances is in-sync.

`pgverify` attempts to solve this by generating per-table hashes from the data and comparing results from multiple targets.

# Getting started

First, clone this repository and run `make build` to generate the `pgverify` binary. Then, run with specified `--targets` as a comma-separated list of PostrgeSQL syntax connection URIs:

```
pgverify --targets postgresql://user1:passwd1@host1:port/database,postgresql://user2:passwd2@host2:port/database
```

The resuling table output will outline which tables are in sync between the targets, and which are not:

```
$ ./pgverify --targets postgres://root@crdb-local:26257/testdb,postgres://postgres@psql-local:5432/testdb --include-tables testtable,tables
+---------------------------+----------------------------------+------------+
|       SCHEMA TABLE        |               HASH               |  TARGETS   |
+---------------------------+----------------------------------+------------+
| crdb_internal.tables      | c57be1fc382cd384f42765f31adade2a | crdb-local |
| information_schema.tables | 6598808f2071cebd826ff340650d2c0a | psql-local |
|                           | c5d56edfe5fd3f9b7ffe9e0a0d763b66 | crdb-local |
| public.testtable          | 006c1b5f7b2b24f5c3711fdea3c3fb8f | crdb-local |
|                           |                                  | psql-local |
+---------------------------+----------------------------------+------------+
```

You can include/exclude specific table names and schemas via CLI flags to eliminate unreconcilable table data from the report.

# Supported databases

| Database Engine     | Supported Versions |
| ------------------- | ------------------ |
| [PostgreSQL][psql]  | `>=10`             |
| [CockroachDB][crdb] | `>=20.2`           |

<!-- Links -->
[crdb]: https://www.cockroachlabs.com/
[psql]: https://www.postgresql.org/
