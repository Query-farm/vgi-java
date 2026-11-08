<p align="center">
  <img src="assets/vgi-logo.png" alt="Vector Gateway Interface logo" width="320">
</p>

<h1 align="center">vgi-java</h1>

<p align="center">
  Serve <a href="https://github.com/Query-farm-haybarn/haybarn">Haybarn</a> catalogs, tables, and functions from a Java process over <a href="https://arrow.apache.org/">Apache Arrow</a> IPC — the Java implementation of the <a href="https://github.com/Query-farm/vgi">VGI (Vector Gateway Interface)</a> protocol.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-java/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-java/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://central.sonatype.com/artifact/farm.query/vgi"><img src="https://img.shields.io/maven-central/v/farm.query/vgi" alt="Maven Central"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Query%20Farm%20Source--Available-blue" alt="License"></a>
</p>

VGI lets [Haybarn](https://github.com/Query-farm-haybarn/haybarn) — Query Farm's independent derived distribution of DuckDB — `ATTACH` a catalog whose schemas, tables, and functions live in an external worker process. The [vgi extension](https://github.com/Query-farm/vgi) speaks an Arrow-IPC RPC protocol to that worker; this library is everything you need to write the worker side in Java. Your code registers functions and tables against a `Worker` builder — the library handles the wire protocol, schema negotiation, batch streaming, pushdown, and transports.

Wire-compatible with the [Python reference implementation](https://github.com/Query-farm/vgi-python) and the [Go port](https://github.com/Query-farm/vgi-go): all three serve the same integration suite against the same C++ extension.

This source uses VGI wire 3.2.0 under `vgi.v3` and requires a native VGI client speaking wire 3.2.x (3.1 peers are rejected). Upgrade clients and workers together; `vgi.v2` clients are incompatible. See [catalog metadata and publication](docs/catalog_publication.md) and, for what 3.2 adds, [filtered scan planning](docs/scan-planning.md).

## What you can serve

- **Catalog tables** — named tables with inline schemas, comments, tags, constraints, foreign keys, and per-column statistics that feed the engine's optimizer.
- **Scalar functions** — annotation-driven (`ScalarFn`): declare a `compute()` method and the parameter annotations generate the spec, bind-time validation, and dispatch.
- **Table functions** — streaming producers with projection pushdown, filter pushdown, row-id, sampling, and time-travel (`AT`) support.
- **Table-in/out functions** — exchange-style streaming transforms over input batches.
- **Table buffering functions** — sink/source functions that buffer all input before emitting (distributed-aggregation style lifecycles: process → combine → finalize).
- **Aggregate functions** — partial aggregation with cross-process state combine.
- **Catalog versioning** — semver data/implementation version negotiation, release manifests, multi-branch tables, transactions, and attach options.
- **Filtered scan planning** — a table that redirects its scan (`read_parquet`, a VGI function, another catalog's table) can see the query's predicates before choosing which files to return, and tag branches with statistics the client prunes by ([docs](docs/scan-planning.md)).
- **Writable tables and transactions** — catalog tables with INSERT / UPDATE / DELETE functions and write result modes, per-attach `supports_transactions` with begin / commit / rollback hooks, the transaction id and ATTACH options (secret ones included) in every function ([docs](docs/writable-tables-and-transactions.md)).
- **Attach tickets** — seal a user's ATTACH (secret options included) so a runner holding their grant can replay it later without seeing an option ([docs](docs/attach-tickets.md)).

## Start a new worker

The fastest way to a new Java worker is the
[**vgi-starter-java**](https://github.com/Query-farm/vgi-starter-java) template: a complete,
tested worker with a scalar, a table function and a paginated REST API example, recipes for
common data-engineering tasks, SQL and unit tests, metadata that scores 100 on
[vgi-lint](https://github.com/Query-farm/vgi-lint-check), CI, Docker, signed release workflows,
and an `AGENTS.md` for coding agents.

```bash
gh repo create my-worker --template Query-farm/vgi-starter-java --private --clone
cd my-worker
make init NAME=my_worker   # rename the catalog, package and binary
make check                 # everything CI runs
```

Or click **Use this template** on GitHub. See
[starter templates](https://query.farm/vgi/docs/concepts/starter-templates/) for details. The
rest of this README covers the SDK itself.

## Requirements

- **Java 21+** at runtime. The shared-memory side-channel (zero-copy batch transfer with a co-located engine) additionally requires **JDK 22+**; on 21 it transparently falls back to pipe transport.
- [Haybarn](https://github.com/Query-farm-haybarn/haybarn) with the [vgi extension](https://github.com/Query-farm/vgi) installed on the client side (it's in Haybarn's signed community channel: `INSTALL vgi FROM community`).

## Installation

Artifacts are published to Maven Central under the `farm.query` group.

**Gradle (Kotlin DSL):**

```kotlin
dependencies {
    implementation("farm.query:vgi:0.46.1")
}
```

**Maven:**

```xml
<dependency>
  <groupId>farm.query</groupId>
  <artifactId>vgi</artifactId>
  <version>0.46.1</version>
</dependency>
```

The RPC layer ([`farm.query:vgirpc`](https://github.com/Query-farm/vgi-rpc-java)) comes in transitively.

## Quickstart

A worker with one scalar function:

```java
import farm.query.vgi.Worker;
import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

public final class DemoWorker {

    /** {@code multiply(value INT64, factor INT64 [const]) -> INT64} */
    static final class Multiply extends ScalarFn {
        @Override public String name() { return "multiply"; }
        @Override public String description() { return "Multiplies a value by a constant factor"; }

        public void compute(@Vector BigIntVector value, @Const long factor, BigIntVector result) {
            int rows = value.getValueCount();
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) {
                    result.setNull(i);
                } else {
                    result.set(i, value.get(i) * factor);
                }
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = Worker.builder()
                .catalogName("demo")
                .registerScalar(new Multiply());
        worker.runFromArgs(args); // stdio by default; --unix / --http via flags
    }
}
```

The `compute()` signature drives everything: `@Vector` parameters are per-row input columns, `@Const` parameters are bind-time constants, `@Setting` parameters read session settings, and the last unannotated Arrow vector is the framework-allocated output.

Scalar functions may override `argumentMonotonicity()` and return a
`List<ArgumentMonotonicity>`. The optional list follows `argumentSpecs()`
declaration order and must contain exactly one entry per declaration. Fixed,
defaulted, and constant arguments each occupy one slot; a vararg declaration
occupies one slot regardless of call-time expansion. Named SQL invocation order
does not reorder these claims. Returning `null` makes no claims.

**The worker JVM needs three flags** — Apache Arrow requires access to `java.nio` internals, Arrow's Netty 4.2 allocator needs `sun.misc.Unsafe` (which Netty switches off by default on Java 25+), and the shared-memory transport uses the FFM API:

```
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
-Dio.netty.noUnsafe=false
--enable-native-access=ALL-UNNAMED
```

With the Gradle `application` plugin, bake them into the start script so the worker binary is self-contained:

```kotlin
application {
    mainClass.set("DemoWorker")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
        "-Dio.netty.noUnsafe=false",
        "--enable-native-access=ALL-UNNAMED",
    )
}
```

Without the `--add-opens` flag the worker fails at first query with `Failed to initialize MemoryUtil`. vgirpc sets the Netty property itself when it is first to touch Arrow, but pass it anyway: without it, anything that allocates Arrow memory first fails with an `ExceptionInInitializerError` from `NettyAllocationManager` on Java 25+.

Attach and query it from Haybarn:

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/path/to/demo-worker');
SELECT demo.multiply(21, 2);  -- 42
```

The `launch:` location scheme starts the worker once behind a flock-coordinated Unix socket and reuses it across queries and engine processes — essential for JVM workers, which are expensive to cold-start. Plain subprocess (`/path/to/worker`) and `http(s)://` locations also work.

## Running several HTTP replicas

Set the same `VGI_SIGNING_KEY` (or call `Worker.signingKey(byte[])`) on every
replica. It seals `attach_opaque_data` and, through an HKDF-SHA256 derivation
(label `vgi.http.state_token_key.v1`, never the raw key), keys the HTTP
stream-state tokens, so a table-function stream started on one replica resumes
on any other and survives a restart. Without it each process uses a random key
and logs a warning at startup. An explicit `HttpServer.Config.Builder.tokenKey`
still takes precedence.

## Example worker

For authenticated `iroh://` and HTTP-semantics `httpi://` deployments, see
[Iroh workers and clients](docs/iroh.md). To serve other vgi-rpc protocols
beside `vgi.v3`, or to answer token introspection for a reverse proxy, see
[Hosting extra protocols, and token introspection](docs/hosted-protocols.md).
To let a runner reattach a user's catalog later with only that user's grant and
a sealed ticket (`vgi.attach_tickets.v1`), see [Attach tickets](docs/attach-tickets.md).

The [`vgi-example-worker`](vgi-example-worker/) module (not published) is a complete worker with 90+ functions — scalar, table, aggregate, table-in/out, buffering, partitioned, multi-branch, transactional — that serves the canonical VGI integration suite. It is the best place to look for working patterns of any feature.

## Related projects

| Repository | What it is |
|---|---|
| [Query-farm-haybarn/haybarn](https://github.com/Query-farm-haybarn/haybarn) | Haybarn — the independent derived distribution of DuckDB by Query Farm |
| [Query-farm/vgi](https://github.com/Query-farm/vgi) | The vgi engine extension (C++) — the client side of the protocol |
| [Query-farm/vgi-python](https://github.com/Query-farm/vgi-python) | Python reference implementation of the worker side |
| [Query-farm/vgi-go](https://github.com/Query-farm/vgi-go) | Go implementation of the worker side |
| [Query-farm/vgi-rpc-java](https://github.com/Query-farm/vgi-rpc-java) | The transport-agnostic Arrow RPC framework this library builds on |

## License

[Query Farm Source-Available License, Version 1.0](LICENSE).
