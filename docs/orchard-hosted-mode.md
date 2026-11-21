# Orchard hosted mode

`VGI_ORCHARD_HOSTED=1` runs the worker as an Orchard *Field Worker*. The
hosted base image sets it; you do not set it per deployment. Callers present a
*Pass*: a short-lived [RFC 9068](https://www.rfc-editor.org/rfc/rfc9068) JWT
access token issued by the Orchard Front Desk for one canonical Desk Address.
The worker verifies every Pass locally and accepts no other credential. The
full design is in the extension repository's `docs/location-delegation.md`.

Hosted mode needs no code. Any worker whose `main` calls `Worker.runFromArgs`
(or `Worker.runHttp`) picks it up from the environment:

```bash
VGI_ORCHARD_HOSTED=1 \
VGI_JWT_ISSUER=https://auth.queryfarm.cloud \
VGI_JWT_AUDIENCE=https://desk.queryfarm.cloud/d/dep_123 \
VGI_JWT_JWKS_URI=https://auth.queryfarm.cloud/.well-known/jwks.json \
VGI_SIGNING_KEY=... \
java -jar my-worker.jar --http --host 0.0.0.0 --port 8080
```

## Startup fails closed

The worker prints an `Error: VGI_ORCHARD_HOSTED=1 (Orchard hosted mode): ...`
line and exits with status 1 unless all three of `VGI_JWT_ISSUER`,
`VGI_JWT_AUDIENCE` and `VGI_JWT_JWKS_URI` are set. Unset and empty are both
errors. Each takes exactly one value: there is no comma splitting, and a value
containing a comma is refused. `VGI_SIGNING_KEY` (or `Worker.signingKey(...)`)
must also supply a stable key of at least 32 bytes, because the key seals
catalog opaque data and HTTP state tokens and a Field Worker's machines restart.
A key a parent process minted for its children (`VGI_SIGNING_KEY_MINTED=1`) does
not count.

It also refuses to start when any other way in is configured:

- `VGI_BEARER_TOKENS`;
- sealed-grant keys (`VGI_RPC_GRANT_KEYS`, `--grant-key` or `Worker.grantKeys(...)`);
- `Worker.resolveToken(...)` or `Worker.mintGrant(...)`;
- `VGI_OAUTH_RESOURCE` (a Field Worker never advertises `resource_metadata`);
- `VGI_PROXY_PROOF_MODE` other than `off`, an `HttpServer.Config` with
  `proxyProofRequired` or `proxyAuthHeaders`, or peer identity (Iroh,
  Tailscale, SPIFFE; `--iroh-issuer`);
- an `authenticator(...)` set on the `HttpServer.Config` (for example by the
  `runFromArgs` customizer);
- stdin/stdout, `--unix`, `--tcp` or `--iroh-raw-upstream`, which carry no Pass
  (`runStdio`, `runUnixSocket`, `runTcp` and `runIrohTcpUpstream` refuse too);
- a `VGI_ORCHARD_HOSTED` value other than `1`/`0`/`true`/`false`/`yes`/`no`/`on`/`off`.

## Verification

- algorithm `ES256` only (`VGI_JWT_ALGORITHMS`, comma-separated, overrides;
  `none` and `HS*` are always refused);
- JOSE header `typ` must be `at+jwt` (case-insensitive, `application/` prefix
  optional);
- `iss` must match exactly, and `aud` must name exactly one audience, matched
  exactly;
- `exp` is required, and `exp` / `nbf` are checked with 60 s of clock skew;
- the JWKS is cached for at most 10 minutes, or less if its
  `Cache-Control: max-age` is shorter. When that time is up the keys are
  discarded. If the re-fetch fails, requests get a 503 with `Retry-After` until
  it succeeds; stale keys are never used;
- a Pass with an unknown `kid` triggers a JWKS re-fetch at most once every 60
  seconds. Between re-fetches it is simply rejected.

The verifier is vgi-rpc-java's `JwtAuthenticator` (nimbus-jose-jwt) with its
hardening options turned on; `OrchardHosted.passAuthenticator(...)` builds it.

## Every request needs a Pass

The only exceptions are `HEAD /health` (the Front Desk's liveness probe) and
CORS preflights (`OPTIONS` with `Access-Control-Request-Method`), which browsers
send without credentials. That includes `GET /health`, other `OPTIONS`
requests, the landing and describe pages and unknown paths. A request without a
valid Pass, including one with no `Authorization` header at all, gets:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer error="invalid_token"
VGI-Auth-Reason: missing_credential | invalid_credential | expired_credential
VGI-Supported-Encodings: ...        (and the other VGI capability headers)
```

There is no `resource_metadata` in the challenge. If the JWKS cannot be
fetched the answer is 503 with `Retry-After` instead: the worker cannot tell
whether the Pass is valid, so it does not claim that it isn't. Authentication
runs before the request body is read, size-checked or decompressed
(vgi-rpc-java's `HttpServer.Config.strictAuth`). CORS is open
(`Access-Control-Allow-Origin: *`), because the only credential is the bearer
Pass.

The SDK never logs the `Authorization` header. Jetty's `DEBUG` wire trace does
print request headers, so hosted mode caps the `org.eclipse.jetty` loggers at
`INFO` through slf4j-simple's per-logger property. A worker that ships another
slf4j binding (logback, log4j) must keep `org.eclipse.jetty` at `INFO` or above
in that binding's configuration.

## Function code sees the Pass's claims

Claims reach function code through the usual auth path: `CallContext.auth()`
in table functions and other stream handlers, and `AuthScope.current().auth()`
from a scalar's `compute`. They keep their JSON types (`email_verified` is a
`Boolean`, `exp` a `Long`). `AuthContext.principal()` is `sub` and
`AuthContext.domain()` is `"orchard"`.

| Claim | Meaning |
|-------|---------|
| `sub` | Users: the WorkOS user id. Workloads: an opaque Registry id |
| `kind` | `user` or `workload` |
| `email`, `email_verified` | Users only |
| `name` | The user's name, or the name an admin gave the Workload |
| `org_id`, `org_name` | The workspace that owns the deployment |
| `federated_iss`, `federated_sub` | Workloads signed in through a Trust Rule only |
| `client_id`, `iat`, `nbf`, `exp`, `jti` | Standard token claims |

A valid Pass means "a member, or a Workload, of the workspace that owns this
deployment". Anything finer is the worker's own decision. Code that authorizes
by `sub` or `name` must also check `kind`, because a Workload's id and name
could look like a user's.

The example worker registers `pass_claims(x BIGINT) -> VARCHAR` in hosted mode
only. It returns the whole verified claim set as JSON, for the hosted-mode tests:
`OrchardHostedProcessTest`, and the SDK-neutral harness in the vgi repository
(`test/hosted/run_hosted_conformance.py`), which CI runs through
`ci/run-hosted-conformance.sh` with every non-HTTP transport as a must-refuse
command.
