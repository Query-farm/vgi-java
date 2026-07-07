# HTTP landing-surface conformance

Cross-language conformance for the VGI worker HTTP landing surface (the shared,
byte-identical `landing.html` plus its `describe.json` JSON contract). See the
normative spec at `~/Development/vgi/docs/http-landing-contract.md`.

## Files

| File | Source of truth | Purpose |
|---|---|---|
| `describe.schema.json` | `Query-farm/vgi:test/landing/describe.schema.json` | JSON Schema for `GET /describe.json`. Vendored verbatim. |
| `run_landing_conformance.py` | `Query-farm/vgi:test/landing/run_landing_conformance.py` | The language-agnostic runner (drives a live worker over HTTP). Vendored verbatim. |
| `run.sh` | this repo | Boots the built example worker and runs the runner against it. |

Both vendored files are byte-copies of the upstream `vgi` repo; refresh them
when the contract changes.

## Run locally

```bash
./gradlew :vgi-example-worker:installDist   # with VGI_RPC_JAVA_DIR if building vgirpc from source
python -m pip install jsonschema
VGI_WORKER_BIN=vgi-example-worker/build/install/vgi-example-worker/bin/vgi-example-worker \
  ci/landing/run.sh
```

## What it checks

- `GET /describe.json` validates against `describe.schema.json`.
- `GET /` (Accept: text/html) serves the pinned `landing.html` (asserts the
  `vgi-landing-asset vN` marker).
- `GET /?format=json` returns a JSON status object.
- `GET /describe/{catalog}/{schema}/{table}.json` returns valid columns for one
  table and one view per schema.

## Golden

The runner supports a normalized `--golden` diff, but the Python ExampleWorker
golden is **not** enforced against Java — Java's example worker exposes a
different catalog. Only the schema + marker + column checks run in CI. To
generate a Java-specific golden for local review:

```bash
python ci/landing/run_landing_conformance.py --url http://localhost:PORT \
  --golden ci/landing/describe.java.expected.json --update
```
