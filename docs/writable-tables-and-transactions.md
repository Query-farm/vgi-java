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

**Secret options** (`AttachOptionSpec.asSecret()`) reach functions and hooks this way too; it
is the standard mechanism, and catalog code never puts a secret into its own attach bytes or keeps
it server-side. They never appear in plaintext in the attach value, on any transport and in the
opened (inner) bytes on HTTP (`vgi-opaque-data-sealing.md` rule 5). The framework encrypts them:

- **AEAD:** ChaCha20-Poly1305, blob `version(0x01) || nonce || ciphertext || tag`, AAD
  `"vgi.attach_secret_options.v1" 0x00 || attach uuid` (a blob moved into another attach does not
  open).
- **Key:** `HKDF-SHA256(ikm = root, salt = empty, info = "vgi.attach_secret_options.v1", L = 32)`;
  the root itself is never the AEAD key. The root is, in order:
  1. the configured key (`Worker.signingKey`, `Worker.opaqueDataKey` or `VGI_SIGNING_KEY`), else
  2. over HTTP, the per-process sealing key (multi-replica deployments configure a key anyway), else
  3. on stdio / unix sockets / launch pools, the **host-local key**, shared with the workers of
     every VGI SDK on the host: `$VGI_ATTACH_SECRET_KEY_FILE` if set, else
     `$XDG_RUNTIME_DIR/vgi/attach-secret-options.key` if that directory exists, else
     `<per-user temp dir>/vgi-<user>/attach-secret-options.key` (`$TMPDIR`, else
     `java.io.tmpdir`; on macOS the per-user `/var/folders/...`). 32 raw random bytes; directory
     `0700`, file `0600`, both owned by the user, refused otherwise; created atomically (temp file,
     then hard link) so concurrent first starts agree; never overwritten; read only when a secret
     option actually has a value.
- **Failure:** an attach whose blob does not open under this worker's key (another deployment, a
  host key file deleted or rotated mid-session, a moved blob) is refused with the uniform
  `attach_opaque_data not recognized` error (`INVALID_ARGUMENT`, `opaque_data_not_recognized`);
  the client re-ATTACHes. A secret option given a value when no key can be obtained fails the
  `ATTACH` with a message naming `VGI_SIGNING_KEY` / `VGI_ATTACH_SECRET_KEY_FILE`.
- Secret values are never logged. `AttachOptions.toString()` redacts them, and
  `AttachOptions.fromAttachId(attachId)` (a decode without the framework) never sees them
  (`secretsAvailable()` is then false).
- **Keep secrets out of function state.** A producer or exchange that needs a secret while
  producing rows stores `params.attachId()` in its state and calls `AttachOptions.open(attachId)`
  when it needs the values; it never stores the secret (state travels to the client, sealed, on
  HTTP).

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

**Function visibility (protocol 3.3).** The framework always lists every function a table
names (scan, insert, update, delete) in `catalog_schema_contents_functions`, so the extension
always has their capabilities (projection / filter pushdown, required secrets, `has_finalize`,
...). Each listing entry carries `FunctionInfo.visibility`:

- write functions are always `table_only`: absent from `duckdb_functions()`, not callable from
  SQL, used only to run the table's INSERT / UPDATE / DELETE;
- a table's scan function is `table_only` too unless the table opts in with
  `CatalogTable.Builder.exposeScan(true)`, which makes it an ordinary public function
  (`SELECT * FROM cat.main.items_scan()` then works);
- `Worker.registerUnlistedTable` keeps working and now means "list as `table_only`" (it is no
  longer needed for a scan a table names, which is `table_only` by default);
- every other function is `public`.

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
