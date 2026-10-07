# Attach tickets

An attach ticket lets a runner reattach a user's catalog later, as that user,
without ever seeing the user's attach options. The secret options are included.
The normative spec is vgi-python's
[`docs/protocol/vgi-attach-tickets.md`](https://github.com/Query-farm/vgi-python/blob/main/docs/protocol/vgi-attach-tickets.md).
The cross-SDK vectors are
`attach_ticket_vectors.json`, which this repo's `AttachTicketVectorsTest`
consumes.

A ticket says *what* to attach. A sealed grant
([`issue_grant`](hosted-protocols.md#sealed-grants-grants-that-log-in)) says
*who* attaches:

1. The user attaches and is logged in. The client calls `seal_attach` on
   `vgi.attach_tickets.v1`, and the worker seals the catalog name, the options
   and the version specs into a `vgia1.` ticket.
2. The client calls `issue_grant` for a `vgig1.` grant.
3. Later, a runner presents `Authorization: Bearer <grant>` and
   `ATTACH … (attach_ticket '<ticket>')`. The extension sends the ticket as the
   only option, `vgi_attach_ticket`. Before any catalog code runs, the worker
   opens the ticket under the caller's principal and attaches with the sealed
   options.

A ticket carries no authority. It opens only for the principal it was sealed
for, so without that principal's grant (or login) it attaches nothing.

## Turning it on

The worker hosts `vgi.attach_tickets.v1` (protocol version `1.0.0`) only when
**all** of these hold. Otherwise the protocol is absent, not hosted-and-refusing,
so a client learns the answer from reflection.

- **The transport is HTTP.** Other transports carry no caller identity.
- **A signing key is configured.** Use `VGI_SIGNING_KEY`, or
  `Worker.signingKey(byte[])` / `Worker.opaqueDataKey(byte[])` in code. A
  32-byte key is used as-is. Any other length is replaced by its SHA-256, the
  same normalization every SDK applies to `VGI_SIGNING_KEY`, so give every
  replica the same value. The per-process random key a worker uses without
  configuration never enables tickets, because every ticket would die on
  restart. The same key also seals `attach_opaque_data`.
- **The worker can issue grants.** Either grant keys are configured
  (`VGI_RPC_GRANT_KEYS`, `--grant-key`, `Worker.grantKeys`) or the worker sets
  `Worker.mintGrant`.

```bash
export VGI_SIGNING_KEY="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
export VGI_RPC_GRANT_KEYS="$(head -c 32 /dev/urandom | base64)"
my-worker --http
```

## `seal_attach`

`seal_attach(SealAttachRequest) -> AttachTicket`. The request carries
`catalog_name`, `options` (the one-row Arrow IPC record exactly as
`CatalogAttachRequest.options`), `data_version_spec`, `implementation_version`
and `ttl_seconds`.

- **Caller.** An anonymous caller gets `action_denied`. A stale login may seal,
  because a ticket carries no authority.
- **Validation.** Every failure below is reported together as
  `invalid_request`, one `BadRequest` field violation per failure:
  - a negative `ttl_seconds`;
  - an options record with more than one row;
  - an unknown catalog;
  - an option the catalog does not declare (names compared case-insensitively);
  - a missing required option;
  - a nested `vgi_attach_ticket`;
  - options over 16 KiB.
- **Lifetime.** The worker's grant maximum is the grant keys' `maxTtlSeconds`,
  else `VGI_RPC_GRANT_MAX_TTL_SECONDS`.
  - `ttl_seconds = 0` means as long as that maximum allows, or no expiry
    (`expires_at = +Infinity`) when there is no maximum.
  - A positive TTL is capped at the maximum.

## Redemption

When a `catalog_attach` request's options contain `vgi_attach_ticket` (any
letter case), the framework handles it before routing on the catalog name:

- **Another option alongside the ticket** is `invalid_request`. This rule is
  checked before the ticket is opened.
- **A ticket that does not open** is `attach_ticket_invalid`. That covers
  tampering, the wrong principal, an anonymous caller, another worker's key,
  and a worker with no key.
- **An authentic ticket outside its lifetime** (60 s skew) is
  `attach_ticket_expired`.
- **Otherwise** the request becomes the one the user made: the sealed name,
  options and version specs, keeping `client_capabilities`.

Neither the ticket nor a restored option is logged or echoed in an error.

`vgi_attach_ticket` is a reserved attach-option name. Declaring an
`AttachOptionSpec` with that name, in any letter case, throws.

The format and verification live in `farm.query.vgi.AttachTickets`. The
envelope is `0x01 || nonce(24) || XChaCha20-Poly1305(payload)` with AAD
`"vgi.attach_ticket.v1" 0x00 || principal`. It binds the principal, not the
auth domain, because a ticket is sealed under a login and opened under a grant.

## The `ticket_probe` fixture

The example worker serves the cross-SDK `ticket_probe` catalog
(`TicketProbeFixture`). Its options are `region` (VARCHAR, default
`'us-east-1'`) and `api_key` (VARCHAR, required, secret). Its table
`main.probe` returns one row: `region` and `api_key_sha256`, the first 12 hex
characters of SHA-256 of the key.

The example worker's HTTP server accepts `Bearer vgi-test-alice` and
`Bearer vgi-test-bob` as fresh logins, so a client can call `issue_grant` with
nothing but a bearer. With `VGI_RPC_GRANT_KEYS` set, a `vgig1.` bearer is
verified as a grant. Any other bearer stays anonymous.
