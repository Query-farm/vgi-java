# Direct object storage (no blob vendor)

A self-hosted worker can keep large values in the operator's own bucket (AWS S3, any
S3-compatible service, or Google Cloud Storage) instead of the blob vendor: externalized
results, upload grants and argument references, StateRef objects, and function storage
values. The worker signs the URLs itself. Configuration is by environment only, with the same
names and behaviour in every VGI SDK (direct object storage contract v1).

## Selecting a backend

`VGI_STORAGE_BACKEND` = `vendor` | `s3` | `gcs` | `none`.

Unset: `vendor` if `VGI_BLOB_VENDOR_URL` is set, else `s3` if `VGI_STORAGE_S3_BUCKET` is set,
else `gcs` if `VGI_STORAGE_GCS_BUCKET` is set, else `none` (everything inline). Setting more
than one of those three without `VGI_STORAGE_BACKEND` refuses to start and names the conflict.
Orchard hosted mode (`VGI_ORCHARD_HOSTED=1`) normally uses the vendor; `s3`/`gcs` with it is
allowed and logged.

`s3` and `gcs` need the backend module on the worker's classpath:
`farm.query:vgirpc-s3` or `farm.query:vgirpc-gcs` (the example worker carries both). Without it
the worker refuses to start and names the module.

## S3

| Variable | Meaning | Default |
|---|---|---|
| `VGI_STORAGE_S3_BUCKET` | bucket (required) | |
| `VGI_STORAGE_S3_PREFIX` | key prefix, e.g. `vgi/`; objects go under `<prefix>rpc/` (results, upload grants, StateRef and argument objects) and `<prefix>fs/` (function storage) | empty |
| `VGI_STORAGE_S3_ENDPOINT` | custom endpoint, `https://` or `http://` (MinIO, Ceph, R2's S3 API, Wasabi, B2, LocalStack) | AWS |
| `VGI_STORAGE_S3_REGION` | signing region (`auto` for R2) | `AWS_REGION`, `AWS_DEFAULT_REGION`, `us-east-1` |
| `VGI_STORAGE_S3_FORCE_PATH_STYLE` | bucket in the path | `true` with an endpoint |
| `VGI_STORAGE_S3_PUBLIC_ENDPOINT` | endpoint for the presigned URLs handed out (the worker must reach it too: request-upload pointers are fetched by URL) | the endpoint |
| `VGI_STORAGE_S3_CHECKSUM` | `sha256`, or `none` for services that ignore `x-amz-checksum-sha256` | `sha256` |
| `VGI_STORAGE_S3_SDK_CHECKSUM` | the AWS SDK's own CRC32 request checksums: `when_required` or `when_supported` | `when_required` with an endpoint, else `when_supported` |
| `VGI_STORAGE_S3_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` / `_SESSION_TOKEN` | explicit credentials | the AWS default chain |
| `VGI_STORAGE_S3_SESSION_EXPIRES` | the explicit session token's expiry (RFC 3339 or epoch seconds), so URL lifetimes are capped; without it a token logs a warning | none |
| `VGI_STORAGE_ALLOW_PLAIN_HTTP` | `1` allows an `http://` endpoint or public endpoint (refused otherwise; the extension has its own `vgi_storage_allow_plain_http`) | refuse |
| `VGI_STORAGE_URL_TTL_SECONDS` | presigned URL lifetime | 3600 |
| `VGI_STORAGE_S3_VERIFY` | `1` = check the bucket at startup | check on first use |
| `VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS` | a transfer fails only after this long without bytes moving | 30 |

Credentials: the explicit variables when set, else the AWS SDK's default chain (environment,
shared profile including `role_arn`/`source_profile` and `credential_process`, web identity,
ECS/EKS container credentials, EC2 IMDSv2). Temporary credentials are refreshed before they
expire. A presigned URL stops working when its signing credentials expire, so its lifetime is
`min(VGI_STORAGE_URL_TTL_SECONDS, credential expiry - now - 60 s)`; upload grants ask for at
least 15 minutes (a shorter session is refreshed once, then URLs are signed for what is left, with a warning). Stored objects are addressed by key, not URL, so StateRefs and argument
references survive credential rotation.

The bucket check is a HEAD on the bucket and fails with what to fix: the bucket does not exist,
access is denied, or the bucket is in another region (naming it).

### `VGI_STORAGE_S3_CHECKSUM=none`

For a service that does not enforce `x-amz-checksum-sha256`: the worker issues no upload grants,
so argument references and large inputs stay inline and `VGI-Upload-URL-Support` /
`VGI-Arguments-Ref-Bytes` are not advertised (requests over `VGI_RPC_MAX_REQUEST_BYTES` are
refused). Results are still externalized (the extension verifies their sha256) and StateRef and
function-storage objects are still stored (the worker verifies them on fetch). Logged at startup.

## GCS

`VGI_STORAGE_GCS_BUCKET` (required), `VGI_STORAGE_GCS_PREFIX`, `VGI_STORAGE_GCS_ENDPOINT` (an
emulator or private endpoint; the API and the signed URLs use it), credentials from
`VGI_STORAGE_GCS_CREDENTIALS_FILE` or Application Default Credentials. V4 signed URLs,
`VGI_STORAGE_URL_TTL_SECONDS`, `VGI_STORAGE_GCS_VERIFY=1` checks the bucket at startup (else on first use), and an `http://` endpoint needs `VGI_STORAGE_ALLOW_PLAIN_HTTP=1`. Keys use the same `rpc/` / `fs/` layout. GCS cannot enforce a SHA-256 on an upload, so the GCS backend
issues no upload grants (as with `CHECKSUM=none`).

## Lifecycle

Add bucket lifecycle rules: 1 day on `<prefix>rpc/` and 2 days on `<prefix>fs/`, as the vendor does.
The worker deletes function-storage values it no longer needs, best effort (failures are ignored);
everything else is left to the lifecycle rules.

## In code

`Worker.storageSettings(BlobVendorHttpSettings.forStorage(storage, uploadUrlProvider,
urlValidator, env))` wires any storage (for example an `S3Storage` built with its builder)
instead of the one the environment selects; `StorageBackend.httpSettingsFromEnv(env)` is what
the worker uses by default.
