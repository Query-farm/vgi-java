# Documentation examples

The workers embedded verbatim in the
[vgi-java documentation](https://query.farm/vgi/docs/java/) — one per function
kind, plus a catalog and a combined worker.

Distinct from [`vgi-example-worker`](../../vgi-example-worker), which holds the
fixtures the C++ integration suite drives; those are written to exercise the
protocol, not to be read.

## Running them

```bash
# From the repo root:
./gradlew :examples:docs:installDist

# Print the ATTACH line and the demo queries:
examples/docs/run.sh

# Serve on a Unix socket in the foreground:
examples/docs/run.sh --serve

# Check every example against a real engine:
HAYBARN=/path/to/haybarn examples/docs/verify.sh
```

Each single-kind worker is independently runnable:

```bash
./gradlew :examples:docs:runScalar --args="--unix /tmp/s.sock --idle-timeout 30"
```

## Two things these examples exist to prove

**`-parameters` is not optional.** The annotation API derives the SQL signature
from parameter *names*, which `javac` erases by default. Without the flag the
worker still starts and still answers positional calls — but arguments are named
`arg0` and `upper_case(value := 'hello')` stops resolving. `verify.sh` checks
both the name and the named call, which is the only way the omission is caught.

**A pooled `launch:` worker outlives a rebuild.** `launch:` reuses one JVM across
queries, which is essential because a cold JVM costs seconds — and means that
after `installDist` the old build keeps answering until it idles out. `verify.sh`
kills stragglers before it runs for exactly this reason.

## Dependency

These build against `project(":vgi")` so they track the SDK at HEAD. A reader
does the same thing with one line:

```kotlin
implementation("farm.query:vgi:0.26.1")
```
