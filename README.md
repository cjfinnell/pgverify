# pgverify

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/cjfinnell/pgverify/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/cjfinnell/pgverify/tree/main)
[![codecov](https://codecov.io/gh/udacity/pgverify/branch/main/graph/badge.svg?token=LsTntCpkUr)](https://codecov.io/gh/udacity/pgverify)
[![Go Report Card](https://goreportcard.com/badge/github.com/cjfinnell/pgverify)](https://goreportcard.com/report/github.com/cjfinnell/pgverify)

`pgverify` is a tool for verifying consistency of data between different database engines.

# Why?

Migrating database engines can be a huge headache; newer relational databases often try to mitigate this by asserting some level of PostgreSQL syntax compatability, but there can be small differences in data types, output formats, etc that make it difficult to affirmatively verify that the actual data between instances is in-sync.

`pgverify` attempts to solve this with a suite of various tests executed against the specified targets and compared for consistency, many of which generate per-table hashes from the data.

# Getting started

First, clone this repository and run `make build` to generate the `pgverify` binary. Then, run with specified targets as a list of PostrgeSQL syntax connection URIs:

```
$ pgverify postgresql://user1:passwd1@host1:port/database postgresql://user2:passwd2@host2:port/database [...]
```

The resuling table output will outline which tables are in sync between the targets, and which are not:

```
$ ./pgverify \
		--tests bookend,full,rowcount,sparse \
		--include-tables testtable1,testtable2,testtable3 \
		--aliases cockroachdb/cockroach:latest,cockroachdb/cockroach:v21.2.0,postgres:10,postgres:11,postgres:12.6 \
		postgres://root@crdb-latest:26257/testdb \
		postgres://root@crdb-21-2-0:26258/testdb \
		postgres://postgres@psql-10:5432/testdb \
		postgres://postgres@psql-11:5433/testdb \
		postgres://postgres@psql-12-6:5434/testdb
+--------+------------+----------------------------------+----------------------------------+----------+----------------------------------+-------------------------------+
| schema |   table    |             bookend              |               full               | rowcount |              sparse              |            target             |
+--------+------------+----------------------------------+----------------------------------+----------+----------------------------------+-------------------------------+
| public | testtable1 | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | cockroachdb/cockroach:latest  |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | cockroachdb/cockroach:v21.2.0 |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:10                   |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:11                   |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:12.6                 |
|        | testtable2 | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | cockroachdb/cockroach:latest  |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | cockroachdb/cockroach:v21.2.0 |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:10                   |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:11                   |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:12.6                 |
|        | testtable3 | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | cockroachdb/cockroach:latest  |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | cockroachdb/cockroach:v21.2.0 |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:10                   |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:11                   |
|        |            | 354c0eedeecf8907a90dd98ed3d826d9 | bffe76957644e5755db2dd2f608bfdb3 |       50 | 454ce26f516555a103be83011b043dfd | postgres:12.6                 |
+--------+------------+----------------------------------+----------------------------------+----------+----------------------------------+-------------------------------+
```

See `pgverify --help` for flag configuration options.

# Supported databases

| Database Engine     | Supported Versions |
| ------------------- | ------------------ |
| [PostgreSQL][psql]  | `>=10`             |
| [CockroachDB][crdb] | `>=21.2`           |

# Test modes

| Test mode  | Description                                                                                                 |
| ---------- | ----------------------------------------------------------------------------------------------------------- |
| `full`     | Generates an MD5 hash from *all* of the rows in a table. Memory intensive, but the highest confidence test. |
| `bookend`  | Generates an MD5 hash from the first and last `X` rows in a table, configured by `--bookend-limit X`.       |
| `sparse`   | Generates an MD5 hash from approximately `1/X` rows in a table, configured by `--sparse-mod X`.             |
| `rowcount` | Simply queries and compares total row count for a table.                                                    |

# Gotchas

* Due to PostgreSQL and CockroachDB having slightly differing ways of sorting keys in a `jsonb` value, this tool uses `length(jsonb::text)` as a low-fidelity proxy fingerprint.

<!-- Links -->
[crdb]: https://www.cockroachlabs.com/
[psql]: https://www.postgresql.org/
