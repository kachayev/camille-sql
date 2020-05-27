# [fit] Talking SQL
# to strangers

<br/>

### Oleksii Kachaiev, @kachayev

---

# SQL as a Language

* old invention, popularized by RDMS in 70s

* a few years of being "not cool"
	
	* collateral damage of NoSQL movement?

* what do we have now?

	* databases that never talked SQL (Druid, Cassandra[1] etc)

	* not even databases (Spark, Flink, Kafka etc)

[1] limitations applied

---

# SQL as an API

* GraphQL is a **query language** made it to be an **API**

	* (probably for a wrong reason)

* Strata [talk](https://speakerdeck.com/kachayev/managing-data-chaos-in-the-world-of-microservices) comparing GraphQL and SQL

	* (mostly recognized as a smart trolling)

* this talk: **how** to expose SQL API
	
	* (dealing with **why** as we go)

--- 

# SQL as an API: Demo

* [camille-sql](https://github.com/kachayev/camille-sql)

* running SQL queries over local Maven artifacts

---

# Maven Artifacts Tree

```shell
% tree ~/.m2/repository/ | less
/Users/okachaiev/.m2/repository
├── aleph
│   └── aleph
│       └── 0.4.7-alpha5
│           ├── _remote.repositories
│           ├── aleph-0.4.7-alpha5.jar
│           ├── aleph-0.4.7-alpha5.jar.sha1
│           ├── aleph-0.4.7-alpha5.pom
│           └── aleph-0.4.7-alpha5.pom.sha1
├── ant
│   └── ant
│       └── 1.6.5
│           ├── _remote.repositories
│           ├── ant-1.6.5.jar
│           ├── ant-1.6.5.jar.sha1
│           ├── ant-1.6.5.pom
│           └── ant-1.6.5.pom.sha1

```

---

# camille-sql: Running Server

```shell
% bin/camille-server
[INFO] Scanning for projects...
[INFO]
[INFO] -----------------< org.okachaiev.camille:camille-sql >------------------
[INFO] Building Camille SQL 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] >>> exec-maven-plugin:1.2.1:java (default-cli) > validate @ camille-sql >>>
[INFO]
[INFO] <<< exec-maven-plugin:1.2.1:java (default-cli) < validate @ camille-sql <<<
[INFO]
[INFO]
[INFO] --- exec-maven-plugin:1.2.1:java (default-cli) @ camille-sql ---
Artifacts repository path: /Users/okachaiev/.m2/repository/
Running server on localhost:26727
```

---

# camille-sql: PSQL Client

```shell
% PGPASSWORD=nopass psql "sslmode=disable host=localhost port=26727"
psql (12.2, server 9.5.0)
Type "help" for help.

okachaiev=>
```

---

# camille-sql: Queries

```sql
okachaiev=> select * from artifacts limit 6;
    uid     |         group_id         |       artifact_id        |        name         |                   url
------------+--------------------------+--------------------------+---------------------+------------------------------------------
 3227713579 | alandipert               | desiderata               | desiderata          | https://github.com/alandipert/desiderata
 3382955103 | aopalliance              | aopalliance              | AOP alliance        | http://aopalliance.sourceforge.net
 1507835947 | asm                      | asm-parent               | ASM                 | http://asm.objectweb.org/
 226341444  | backport-util-concurrent | backport-util-concurrent | Backport of JSR 166 | http://backport-jsr166.sourceforge.net/
 1712481681 | biz.aQute                | bndlib                   | BND Library         | http://www.aQute.biz/Code/Bnd
 2280883480 | biz.aQute.bnd            | biz.aQute.bndlib         | biz.aQute.bndlib    | https://bnd.bndtools.org/
(6 rows)

okachaiev => select * from versions where filesize > 10000 limit 5;
    uid     | version | filesize |      last_modified      |                   sha1
------------+---------+----------+-------------------------+------------------------------------------
 3345961009 | 1.3.2   | 337129   | 2019-07-04 23:36:26.464 | ff84d15cfeb0825935a170d7908fbfae00498050
 1053708643 | 1.0.1   | 26514    | 2019-07-04 23:23:20.322 | 49c100caf72d658aca8e58bd74a4ba90fa2b0d70
 2740841946 | 1.6.5   | 1034049  | 2019-07-05 05:37:10.953 | 7d18faf23df1a5c3a43613952e0e8a182664564b
 925895164  | 0.4.4   | 42645    | 2020-02-01 06:45:59.599 | 2522f7f1b4bab169a2540406eb3eb71f7d6e3003
 136773645  | 1.9     | 263965   | 2019-07-04 23:25:30.09  | 9ce04e34240f674bc72680f8b843b1457383161a
(5 rows)
```

---

# camille-sql: Queries

```sql
okachaiev=> SELECT group_id, COUNT(*) AS n_files
okachaiev-> FROM artifacts
okachaiev-> LEFT JOIN versions ON artifacts.uid=versions.uid
okachaiev-> GROUP BY group_id
okachaiev-> ORDER BY n_files DESC
okachaiev-> HAVING n_files > 20;
          group_id          | n_files
----------------------------+---------
 org.apache.hadoop          | 84
 org.apache.maven           | 54
 org.codehaus.plexus        | 50
 org.apache.commons         | 49
 com.fasterxml.jackson.core | 45
 com.twitter                | 44
 com.nimbusds               | 38
 org.typelevel              | 37
 org.scala-lang             | 31
 org.apache.maven.doxia     | 24
(10 rows)
```


---

# Building Blocks

* server/client protocol

* SQL query lifecycle

	* parser

	* planner

	* optimizer

	* execution

* bridge between the two

---

# Protocol

* the goal: to choose wisely to have clients

* e.g. you have a good client for HTTP: browser

* not the case for SQL, databases use different protocols

* JDBC makes them look similar in Java

	* (but we're looking a layer deeper)

---

# Protocol: PGWIRE

* originally PostgreSQL

* own low-level binary serialization format 

* own somewhat documented control flow

* why?

	* clients: `psql`, SQL IDEs, BI

	* adoption outside PostgreSQL: Cockroach, Materialize

	* high performance streaming for large datasets


---

# Sequence Diagram (simplified)

![right fit](file:///Users/kachayev/Desktop/pgwireseq.png)

---

# Protocol: Ignoring Today

* SSL handshake (custom)

* authentication handshake

* prepared statements & bindings

* data copy

* cursors, suspensions, cancellation

* (and a lot more)

---

# Protocol: Implementation

* [Netty](https://netty.io) to run TCP server

* custom `Codec` to deal with network serialization

	* sequence of `Handler`s

	* mapping between transport and application level logic

* `Handler` to orchestrate main loop: get query, execute, send result set

* async i/o is an absolute overkill in this case

--- 

# Protocol: Example Serialization

```java
public ByteBuf toByteBuf(ByteBufAllocator allocator) {
    final ByteBuf buf = allocator.buffer(12 + this.totalLength);
    buf.writeByte('E');                  // not included in length
    buf.writeInt(11 + this.totalLength); // length, self-included
    buf.writeByte('S');                  // magical constant
    buf.writeBytes(this.severity.getBytes());
    buf.writeZero(1);                    // null-termination
    buf.writeByte('C');                  // magical constant
    buf.writeBytes(this.code.getBytes());
    buf.writeZero(1);                    // null-termination
    buf.writeByte('M');                  // magical constant
    buf.writeBytes(this.message.getBytes());
    buf.writeZero(2);                    // double null-termination
    return buf;
}
```

---

# Life of a Query: Theory

* parse

* plan

* optimize

* execute

---

# Life of a Query: Practice

* leverage [Apache Calcite](https://calcite.apache.org/)

* not the easiest framework to work with

* alternatives:

	* [Catalyst](https://github.com/apache/spark/tree/master/sql/catalyst) (too much Spark)

 	* [ZetaSQL](https://github.com/google/zetasql) (too young)

---

# Life of a Query: Calcite

* parse ← lexer, dialect

* plan (compile) ← catalog (databases, "tables")

* optimize ← statistics (CBO), rules folding

* execution ← **scan** definitions

---

# Life of a Query: Lexer

* SQL is **standard**, right? well... it depends

* `Lex` is a set of rules that defines parsing

* using `MYSQL_ANSI`: double quotes, case insensitive

* still problematic to deal with

	* expressions like `E"\n"`

	* `;` as a query termination (standard but optional)

---

TBD: result of query parsing

---

# Life of a Query: Catalog

* `schema`: tables, functions, types etc

* `table`: tables, views, temp views etc

* `table` is anything that implements `Table` interface

* declare "statically":

	* 2 tables

	* each table defines own row type

* enough information for compilation

---

# Register Tables (Statically)

```java
this.resolver = new MavenArtifactsResolver(baseFolder);
this.artifactsTable = new MavenArtifactsTable(this.resolver);
this.versionsTable = new MavenArtifactVersionsTable(this.resolver);
...
SchemaPlus rootSchema = calciteConnection.getRootSchema();
rootSchema.add("artifacts", artifactsTable);
rootSchema.add("versions", versionsTable);
```

---

# Table Defines Row Type

```java
protected final RelProtoDataType protoRowType = new RelProtoDataType() {
    public RelDataType apply(RelDataTypeFactory typeFactory) {
        return new RelDataTypeFactory.Builder(typeFactory)
            .add("uid", SqlTypeName.BIGINT)
            .add("group_id", SqlTypeName.VARCHAR, 1023)
            .add("artifact_id", SqlTypeName.VARCHAR, 255)
            .add("name", SqlTypeName.VARCHAR)
            .add("url", SqlTypeName.VARCHAR)
            .build();
    }
};
```

---

TBD: result of query compilation

---

# Life of a Query: Execution

* minimal requirement: definition of "full scan"

```java
@Override
public Enumerable<Object[]> scan(DataContext root) {
    // better replace this with something interesting :)
    return Linq4j.emptyEnumerable();
}
```

---

# Life of a Query: Execution

* result is a `Linq4j` enumerable (collection or dynamic)

* analog of C# `LINQ` (Language Integrated Queries)

* `df.Select(*).Where(_.version > 1).Take(5)`

* years before Spark popularized the idea

---

# Life of a Query: Optimizations

* generic optimizations provided by the framework

* cost-based optimizations

	* opt out by setting `Statistics.UNKNOWN`

* specifics:

	* skip calculating file size (projection pruning)

	* skip reading checksum (projection pruning)

	* jump to subfolder  (predicate push-down)

---

# Projection Pruning

* extremely common technique for big data analytics

	* (parquet, hehe)

<br/>

```java
@Override
public Enumerable<Object[]> scan(
        DataContext root, int[] projects) {
    // now we know what columns are necessary
    return Linq4j.emptyEnumerable();
}
```

---

# Predicate Push-Down

* Calcite supports basic filters push-down

* analysis of predicate is... **non** trivial

<br/>

```java
@Override
public Enumerable<Object[]> scan(DataContext root,
        List<RexNode> filters, int[] projects) {
    // now we also know predicates
    return Linq4j.emptyEnumerable();
}
```

---

# Predicate Push-Down

Exact match:

```sql
select * from artifacts
where group_id = 'nrepl';
```

Exact prefix:

```sql
select * from artifacts
where group_id LIKE 'com.apache.%';
```

---

# Predicate Push-Down

What if...

```sql
select * from artifacts
where group_id = 'nrepl' OR group_id = 'maven';
```

```sql
select * from artifacts
where group_id = 'nrepl' AND group_id = 'maven';
```

What if...

```sql
select * from artifacts
where LOWER(group_id) + CAST("spark" AS VARCHAR) = 'com.apache.spark';
```

---

# Predicates

* do not need to read the code

* analysis is too **rigit** and inflexible

* might be easily "broken" by user or even **compiler**

* imperative code quickly turns into a mess

	* visitor pattern helps, still :(

	* pattern matching, pls

	* logical PL, ideally 

![right fit](file:///Users/kachayev/Desktop/relnodefilter.png)

---

# Optimization: `toRel`

* table as a **relational expression**, e.g.

* if we have files listing as `Scan`

* both `artifacts` and `version` tables are `Project` + `Filter` applied to it

* `JOIN` between them might be folded

---

# Optimization: `toRel`


* nice part of relational algebra: it's an **algebra**

* hardest part of relational algebra: it's an **algebra**


---

# Stitch Them Together

* leverage Avatica (part of Calcite project)

* standard JDBC interface

* registers Calcite as a **driver**

* local & remote connections


---

# [fit] HAVING Q and A

<br/>


### Oleksii Kachaiev, @kachayev
### [camille-sql](https://github.com/kachayev/camille-sql)