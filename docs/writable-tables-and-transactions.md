# Writable tables, transactions and ATTACH options

This page covers the main catalog's per-attach hooks: reading ATTACH options (secret ones
included) in functions, deciding `supports_transactions` per attach, running code at
transaction begin / commit / rollback, and declaring catalog tables DuckDB can write to.
The `txfix` fixture worker (`vgi-example-worker/.../example/txfix/TxFixWorker.java`, which
serves the vgi suite's `writable_tx/*.test`) uses every piece below and nothing else.

## ATTACH options in functions

Declare options with `Worker.attachOptions(...)`. Every table and table in-out function gets
their values in its parameters, at bind and at init:

```java
String token = params.attachOptions().getString("token");               // a secret option
boolean tx   = params.attachOptions().getBoolean("transactional", false);
```

`TableBindParams`, `TableInitParams`, `TableInOutBindParams` and `TableInOutInitParams` all
carry `attachOptions()` (`farm.query.vgi.AttachOptions`). The values travel inside the
attach's `attach_opaque_data`, so a process that never served the `ATTACH` (a pooled launcher,
the subprocess transport, another HTTP replica) sees them too. Keep no per-attach state in
process memory.

**Secret options** (`AttachOptionSpec.asSecret()`) never appear in plaintext in that value
(`vgi-opaque-data-sealing.md` rule 5). The framework encrypts them (ChaCha20-Poly1305, bound to
the attach) with:

1. the configured key (`Worker.signingKey`, `Worker.opaqueDataKey` or `VGI_SIGNING_KEY`), else
2. over HTTP, the per-process sealing key (multi-replica deployments configure a key anyway), else
3. on stdio / unix sockets, a per-user key file shared by the host's worker processes:
   `$XDG_RUNTIME_DIR/vgi/attach-secret-options.key`, else
   `<java.io.tmpdir>/vgi-<user>/attach-secret-options.key` (directory `0700`, file `0600`,
   both owned by the user; `VGI_ATTACH_SECRET_KEY_FILE` names another file). It is created on
   the first attach that gives a secret option a value.

A process holding a different key sees secret options as absent and
`AttachOptions.secretsAvailable()` is false. `AttachOptions.fromAttachId(attachId)` decodes the
non-secret options from a plain attach id without the framework (secrets are never visible
that way). `AttachOptions.toString()` redacts secret values.

## Per-attach settings: `Worker.onAttach`

```java
worker.onAttach((attach, request) ->
        AttachSettings.transactional(attach.options().getBoolean("transactional", false)));
```

The hook runs on every `ATTACH` of the main catalog, after required options are checked. It
sees the catalog name, the plain attach id and every option value. It returns
`AttachSettings(supportsTransactions, supportsTimeTravel)`; a `null` component keeps the
worker-wide default, `Worker.supportsTransactions(boolean)` / `Worker.supportsTimeTravel(boolean)`
(both `true` by default, as in earlier releases). Throwing fails the `ATTACH`.

## Transaction hooks: `Worker.transactionHooks`

On an attach whose result advertised `supports_transactions`, DuckDB calls
`catalog_transaction_begin` / `_commit` / `_rollback` for every transaction, including the
implicit one around each autocommit statement. `TransactionHooks` runs worker code there:

```java
worker.transactionHooks(new TransactionHooks() {
    @Override public byte[] begin(AttachContext attach) { ... return id; }   // default: 16 random bytes
    @Override public void commit(AttachContext attach, byte[] id) { ... }     // throw to fail COMMIT
    @Override public void rollback(AttachContext attach, byte[] id) { ... }
});
```

- `begin` returns the transaction's **plaintext** id. Over HTTP the framework seals it on the
  way out and opens it on every later call, so `commit`, `rollback` and every function of the
  transaction receive exactly these bytes: `transactionId()` on the four parameter types
  above. A function can therefore stage work under the id and the hooks apply or discard it.
- An exception from `commit` fails the client's `COMMIT` (or autocommit statement) with its
  message. Discard the staged work before throwing; do not count on a rollback call after a
  failed commit.
- The hooks get an `AttachContext` with the attach's options, secret ones included.
- They may run on any worker process: keep staged work somewhere all of them reach.

A producer or exchange that needs the id while producing rows keeps it in a field of its
state (state fields survive HTTP turns). `TableBindParams.transactionStorage()` remains a
key/value store scoped to the transaction (it exists for autocommit statements too).

Code-implemented catalogs (`Worker.registerCatalog`) have the same hooks as
`CatalogInterface.supportsTransactions(attachId)` / `transactionBegin` / `transactionCommit` /
`transactionRollback`. Auxiliary catalogs (`Worker.ExtraCatalog`) do not support transactions.

## Writable catalog tables

Name the write functions on the table descriptor:

```java
CatalogTable.builder("main", "items", Schemas.serialize(schemaWithRowId))
        .scanFunction("items_scan")                 // emits rowid, declares projection pushdown
        .updateFunction("items_update")             // a TableInOutFunction in this catalog
        .writeResultMode(WriteOperation.UPDATE, WriteResultMode.ROWS)
        .constraints(records, revision)
        .build();
```

The framework then advertises `write_result_modes` and the inline write functions in
`TableInfo` and answers `catalog_table_{insert,update,delete}_function_get`; a table without
the function for an operation makes DuckDB refuse it ("does not support UPDATE"). UPDATE and
DELETE need a scan function emitting a row-id column (`WriteResults.rowIdField()`) through
projection pushdown.

A write function is an ordinary `TableInOutFunction`. DuckDB passes `write_options`:

```java
WriteResultMode mode = WriteOptions.from(params.arguments()).resultMode();
// bind:
BindResponse.forSchema(Schemas.serialize(WriteResults.resultSchema(mode, userColumns)));
// per input batch, after applying it:
out.emit(WriteResults.resultBatch(mode, userColumns, oldRows, newRows, Allocators.root()));
```

`COUNT` is a plain statement, `ROWS` is `RETURNING`; `CHANGES` (old / new images) is a maximum
that satisfies both. UPDATE receives the `SET` columns (every column with `RETURNING`) plus the
row id.

**Constraint enforcement.** A catalog with a table that carries constraint records *and* a write
function advertises and selects `vgi.constraint.enforce.v1` automatically (the write functions
must keep the table's ENFORCED constraints). A provider's own `Worker.catalogCapabilities`
decision is merged with it; a profile it advertises whose prerequisite only the framework
supplies (enforce without records read) is still selected.
