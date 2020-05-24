# camille-sql

Run SQL over your Maven artifacts.

## What?

`camille-sql` allows you to explore Maven artifacts you have on your local hard drive.

Run the server:

```shell
$ bin/camille-server
Artifacts repository path: /Users/<user>/.m2/repository/
Running server on localhost:26727
...
```

The server understands PostreSQL wire protocol, so you can connect to it using standard `psql` client:

```shell
$ PGPASSWORD=nopass psql "host=localhost port=26727 sslmode=disable"
psql (12.2, server 9.5.0)
Type "help" for help.

camille=>
```

As you can see, `psql` is absolutely sure it talks to PostgreSQL version 9.5.0.

Now you have access to 2 tables: `artifacts` and `versions`. You can run any read-only SQL query: the server supports projections, filtering, grouping, joins, agg functions, sub-queries etc (pretty much all of SQL99).

Basic queries:

```sql
camille=> select * from artifacts limit 5;
    uid     |       group_id        | artifact_id
------------+-----------------------+-------------
 3345961009 | edu.ucla.cs.compilers | jtb
 1053708643 | stax                  | stax-api
 2740841946 | ant                   | ant
 925895164  | nrepl                 | nrepl
 1376528320 | nrepl                 | bencode
(5 rows)
```

```sql
camille=> select * from versions where filesize > 10000 limit 5;
    uid     | version | filesize | last_modified |                   sha1
------------+---------+----------+---------------+------------------------------------------
 3345961009 | 1.3.2   | 337129   | 2019-12-04    | ff84d15cfeb0825935a170d7908fbfae00498050
 1053708643 | 1.0.1   | 26514    | 2019-07-17    | 49c100caf72d658aca8e58bd74a4ba90fa2b0d70
 2740841946 | 1.6.5   | 1034049  | 2019-07-10    | 7d18faf23df1a5c3a43613952e0e8a182664564b
 925895164  | 0.4.4   | 42645    | 2019-04-09    | 2522f7f1b4bab169a2540406eb3eb71f7d6e3003
 136773645  | 1.9     | 263965   | 2019-10-31    | 9ce04e34240f674bc72680f8b843b1457383161a
 (5 rows)
```

Something more complicated:

```sql
camille=>
SELECT group_id, COUNT(*) AS n_files
FROM artifacts
LEFT JOIN versions ON artifacts.uid=versions.uid
GROUP BY group_id
ORDER BY n_files DESC
LIMIT 10;
         group_id         | n_files
--------------------------+---------
 org.apache.flink         | 391
 org.apache.maven         | 245
 org.codehaus.plexus      | 186
 org.apache.hadoop        | 121
 org.apache.maven.doxia   | 108
 org.apache.maven.plugins | 82
 io.netty                 | 67
 org.apache.maven.shared  | 65
 org.apache.lucene        | 64
 org.apache.commons       | 62
(10 rows)
```

## Why?

The project is mainly done out of pure curiosity:
- figure out how does low-level PostgreSQL transport protocol (`pgwire`) look like
- check on practice how simple or hard would it be to implement `pgwire` as a [Netty](https://netty.io/) codec
- implement simple enough but not trivial example of defining relational algebra system using [Apache Calcite](https://calcite.apache.org/)
- it's just fun and looks cool

## Implementation Details

- [Netty](https://netty.io/) to run async I/O server
- Custom "codec" to encode/decode `pgwire` messages (see `pgwire` package). The tricky part of the codec is that very first message has a different structure compared to all following messages (from PostgreSQL documentation: because of purely historical reasons). Channel initializer creates pipeline with `PgwireStartupMessageDecoder` that will eventually remove itself after the first message is succesfully processed.
- Server handler cycles over incomming SQL queries, decoding queries from bytes protocol and serializing result set into a proper sequence of messages (row descriptor -> row data -> command complete).
- "Database" that actually executes query is implemented in `m2sql` package. It exposes JDBC connection, so the server uses standard `java.sql` interface when talking to it (see documentation for [Apache Avatica](https://calcite.apache.org/avatica/) library).
- [Apache Calcite](https://calcite.apache.org/) is used for query parsing, query planning, query optimizaiton. High-level API is used to declare catalog structure, tables, schemas, relations and scanning logic.

## Optimizations

"Precision is the difference between a butcher and a surgeon" (tm)

Implemented optimization:
- prune unused fields: if "filesize" is not queried, we don't need to waste cpu/ram to calculate it
- push-down filtering predicates: jump into a subfolder if prefix is known

Work in progress:
- optimized join of artifacts and versions: we can walk files tree once to retrieve all the information we need

## Contributions

It's a project made for fun. Feel free to implement whatever feature you want and just drop a PR here ;) See TODO list below if you need ideas on what could be helpful (or what is critically missing).

## TODO

- [ ] Network encoding logic baked into DTO object is such a bad idea... Instead of `toByteBuf` method for each message type, the logic should be implemented in a single encoder with dynamic type-based dispatch
- [ ] SSL, password authentication
- [ ] Propage errors (like, wrong queries) to the client instead of re-openning the connection
- [ ] Carry cancel flag around
- [ ] Additional SQL features, like `show databases`, `show tables` (need to register pg_catalog to make this happen)
- [ ] `pgwire` protocol has way more message types that are currently implemented
- [ ] Reject non-read queries (`insert`, `update` etc)
- [x] Push-down predicates for folder traversal (e.g. `group_id LIKE com.apache.%` predicate might be optimited by going directly  to `com/apache/` subfolder)
- [ ] Better CLI for the server (logs, args parser help etc)

## License

Copyright © 2020 `camille-sql`

`camille-sql` is licensed under the MIT license, available at MIT and also in the LICENSE file.
