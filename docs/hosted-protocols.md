# Hosting extra protocols, and token introspection

A VGI worker speaks `vgi.v2`, the protocol the DuckDB/Haybarn extension calls.
It can also serve other [vgi-rpc](https://github.com/Query-farm/vgi-rpc-java)
protocols from the same process, on the same listener, and it can answer
`vgi_rpc.Identity.v1` so a reverse proxy can resolve credentials.

## Extra protocols: `Worker.hostedProtocols`

Give the worker a hook that returns `(protocol, implementation)` pairs:

```java
@ProtocolName("acme.Reports.v1")
public interface Reports {
    String summary(String report);
}

Worker worker = Worker.builder()
        .catalogName("acme")
        // ... register functions ...
        .hostedProtocols(() -> List.of(
                HostedProtocol.of(Reports.class, new ReportsImpl())));
worker.runFromArgs(args);
```

- **Every transport.** The hook's result is hosted on stdin/stdout, AF_UNIX,
  TCP, the Iroh raw upstream and HTTP. Reflection's `list_protocols` reports
  `vgi.v2` first, then these, in the order returned.
- **Called once per server.** Each `run*` entry point (and `rpcServer()`)
  builds one server and calls the hook once while doing so. The hook may read
  configuration or the environment, but what it returns is fixed for the life
  of that server, so reflection output and protocol hashes stay stable.
- **The protocol is the unit of optionality.** You cannot host a subset of a
  protocol's methods. If a capability is optional, make it its own protocol and
  return it or not.
- **Names.** Each protocol needs its own `@ProtocolName`. A name may not
  repeat, may not be `vgi.v2`, and may not use the reserved `vgi_rpc.` prefix.
  Reflection is always hosted, and Identity is turned on by the hooks below.
  Any violation stops the worker at startup with an error that names
  `Worker.hostedProtocols`.
- **`vgi.v2` is unaffected.** Requests are routed on their `vgi_rpc.protocol`
  key with no fallback to the primary. A client that only names `vgi.v2` (the
  extension) dispatches exactly as it would against a single-protocol worker.

Errors from an extra protocol use vgi-rpc's error model. To pick the code, the
reason and the details a client sees, throw
`farm.query.vgirpc.errors.StatusError`:

```java
throw new StatusError("report is being rebuilt", Code.UNAVAILABLE,
        "report_rebuilding", List.of(new ErrorDetail.RetryInfo(30)));
```

## Token introspection: `resolveToken` / `mintGrant`

`vgi_rpc.Identity.v1` lets a reverse proxy that terminates the only public
listener resolve an opaque bearer credential to a principal
(`introspect_token`), and lets an authenticated caller mint a scoped grant
(`issue_grant`).

```java
Worker worker = Worker.builder()
        .resolveToken(token -> {
            ApiKey row;
            try {
                row = apiKeys.lookup(token);              // your own store
            } catch (IOException e) {
                // "I could not find out" -- a 503-style transient answer, not 401.
                throw new AuthUnavailableException("api-key store unreachable", 5, e);
            }
            return row == null ? null : new TokenIdentity(row.principal(), row.label(), 300);
        })
        .introspectPrincipals("edge-proxy");             // or VGI_INTROSPECT_PRINCIPALS
```

- **Absent unless you opt in.** Without `resolveToken` or `mintGrant` the
  protocol is not hosted at all, rather than hosted and refusing every call.
  Only the methods whose hooks you set are hosted, and a client finds out
  which ones through reflection.
- **HTTP only.** The allowlist is a list of principals. stdin/stdout, AF_UNIX
  and TCP have no caller principal to check it against, so Identity is hosted
  on HTTP only.
- **Allowlist required.** With `resolveToken` set, the worker refuses to start
  unless `introspectPrincipals(...)` or `VGI_INTROSPECT_PRINCIPALS`
  (comma-separated) names who may introspect. There is no permissive default.
  Authenticating a caller and letting that caller introspect are different
  capabilities: "any authenticated caller" would let any user resolve any other
  user's credential to its owner. `mintGrant` alone needs no allowlist, because
  a grant is always for the caller.

### Which error to throw

| The hook ... | Throw / return | Caller sees |
|---|---|---|
| got an answer: the credential is unknown | return `null` | `token_unresolved` / `NOT_FOUND`. Definitive, so a caller may cache it |
| could not find out (store down, timeout, 5xx) | `AuthUnavailableException(detail, retryAfterSeconds, cause)` | `identity_unavailable` / `UNAVAILABLE` with `RetryInfo` set to your `retryAfterSeconds`. Transient; a caller must not negative-cache it |
| declines to mint | `GrantRefusedError` | `grant_refused` / `PERMISSION_DENIED` |

For transient failures, throw `farm.query.vgirpc.http.AuthUnavailableException`.
It is the same error an `Authenticator` throws when its backing store is down,
so a hook that calls that store can let it propagate. The framework translates
it to `identity_unavailable` and keeps your retry hint (WIRE_PROTOCOL §16).
`IdentityUnavailableError` works too.

Do not report an outage as `IllegalArgumentException` or an `AuthException`.
Those read as a definitive "unknown". A proxy may negative-cache that answer,
and then a thirty-second store blip locks valid users out for the life of the
cache.

## Sealed grants: grants that log in

`issue_grant` mints a grant that unattended automation later presents as an
ordinary bearer. Configure grant keys and the worker both mints those grants
and accepts them back (IDENTITY_V1_SPEC §9):

```bash
# 32 random bytes, base64. The first key mints; every key listed verifies.
export VGI_RPC_GRANT_KEYS="$(head -c 32 /dev/urandom | base64)"
export VGI_RPC_GRANT_AUDIENCE="sales-prod"        # optional, bound into every token
export VGI_RPC_GRANT_MAX_TTL_SECONDS=86400        # optional, default 7 days
my-worker --http                                  # or --grant-key KEY (repeatable)
```

or in code: `Worker.builder()...grantKeys(new GrantKeys(List.of(key), "sales-prod", 86400))`.

- **Off unless configured.** With no key nothing changes: no `issue_grant` is
  hosted and no new bearer is accepted. A malformed key stops the worker at
  startup.
- **HTTP only**, like the rest of Identity.
- **Minting.** Unless the worker supplies `mintGrant`, a freshly logged-in user
  calling `issue_grant` gets a `vgig1.` token: XChaCha20-Poly1305 sealed,
  carrying principal, scopes, purpose and expiry, and no server-side state.
- **Accepting.** The worker's HTTP authentication runs in this order: your own
  authenticator, then sealed grants, then `resolveToken`. A request with
  `Authorization: Bearer vgig1.…` is authenticated as the user who minted it,
  domain `grant`, with claims `{grant_id, scopes, purpose}`. A bad, expired or
  foreign grant is a 401 and is never passed to `resolveToken`.
- **Grants never mint grants.** A grant-authenticated caller has no
  `auth_time`, so `issue_grant` refuses it (`stale_auth`).
- **Revocation** is by expiry or by removing a key, which revokes every grant
  that key minted. To rotate, put the new key first, keep the old one after it
  until its grants expire, then remove it.

With `resolveToken` set, HTTP authentication also asks it about bearers your
own authenticator does not accept: a resolved credential authenticates as that
identity (domain `token`), `null` is a 401, and `AuthUnavailableException` is a
503 carrying your `Retry-After`. If your authenticator depends on proxy-injected
evidence (a proxy-proof gate, mTLS headers), the server refuses to start rather
than add these as alternatives beside it. In that case compose them yourself:
put the gate around `IdentityBearer.compose(...)` and pass the result wrapped
in `IdentityBearer.explicit(...)`.

