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
psql (12.2, server 0.0.0)
camille=> 
```

Now you have access to 2 tables: `artifacts` and `versions`. You can run any read-only SQL query: the server supports projections, filtering, grouping, joins, agg functions, sub-queries etc (pretty much all of SQL99).

```shell
camille=> select * from artifacts;

camille=> select * from versions;

camille=>
select group_id, count(*) as n_files
from artifacts
left join versions on artifacts.uid=versions.uid
group by group_id
order by n_files desc;

camille=>
select group_id, sum(filesize) as total_size
from artifacts
left join versions on artifacts.uid=versions.uid
group by group_id
order by total_size desc;
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
- projection elimination: if "filesize" is not queried, we don't need to waste cpu/ram to calculate it
- push-down predicates for filtering: jump into a subfolder if prefix is known

Work in progress:
- optimized join of artifacts and versions: we can walk files tree once to retrieve all the information we need

## Contributions

It's a project made for fun. Feel free to implement whatever feature you want and just drop a PR here ;) See TODO list below if you need ideas on what could be helpful (or what is critically missing).

## TODO

- [ ] SSL, password authentication
- [ ] Additional SQL features, like `show databases`, `show tables`
- [ ] `pgwire` protocol has way more message types that are currently implemented
- [ ] Reject non-read queries (`insert`, `update` etc)
- [ ] Push-down predicates for folder traversal (e.g. `group_id LIKE com.apache.%` predicate might be optimited by going directly  to `com/apache/` subfolder)
- [ ] Better CLI for the server (logs, args parser help etc)

## License

Copyright © 2020 `camille-sql`

`camille-sql` is licensed under the MIT license, available at MIT and also in the LICENSE file.
