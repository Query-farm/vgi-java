// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.View;
import farm.query.vgi.internal.VgiServiceImpl;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.transport.StdioTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;

import java.io.IOException;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder + run-loop façade for a VGI worker.
 *
 * <p>Mirrors {@code vgi.Worker} in vgi-go: register functions, configure catalog
 * metadata, then call {@link #runStdio()} or {@link #runHttp(String, int)}.
 */
public final class Worker {

    /** VGI protocol surface version. Mirrors vgi-python {@code protocol_version.txt}.
     *  Emitted as the {@code vgi_rpc.protocol_version} per-request metadata key.
     *
     *  <p>1.1.0 added the nullable {@code schema_name} field to the bind request:
     *  a function name is not a unique key, because the same name may be
     *  registered in more than one catalog schema, so dispatch resolves
     *  {@code (schema_name, function_name)}.
     *
     *  <p>1.4.0 added {@code table_function_plan} (split-based scan planning) plus
     *  {@code split_tokens} / {@code row_limit} on the init request.</p>
     *
     *  <p>1.3.0 added {@code global_functions} / {@code global_function_prefix}
     *  to the {@code catalog_attach} result (positions 14/15, before
     *  {@code resolved_data_version}): functions a worker asks the client to
     *  publish into its global namespace — see {@link #registerGlobalFunctions}
     *  and {@link #globalFunctionPrefix(String)}. A worker that opts out still
     *  carries the fields (empty list, empty prefix): the extension matches the
     *  response schema exactly.</p>
     *
     *  <p>2.1.0 added the {@code catalog_contents} RPC and the trailing
     *  {@code supports_catalog_contents} column on the {@code catalog_attach}
     *  result. A {@code Worker} catalog is declarative and read-only, so it
     *  advertises the RPC by default (see {@link #supportsCatalogContents(boolean)}).
     *
     *  <p>The value is {@link VgiService#PROTOCOL_VERSION}, generated from the
     *  reference with the rest of the {@code vgi.v2} registry, so a worker and
     *  the interface its clients use cannot disagree.</p> */
    public static final String VGI_PROTOCOL_VERSION = VgiService.PROTOCOL_VERSION;

    /** Environment variable holding the worker's signing key; see {@link #signingKey(byte[])}. */
    public static final String SIGNING_KEY_ENV = "VGI_SIGNING_KEY";

    private String catalogName = "vgi";
    private String catalogComment = "";
    private final Map<String, String> catalogTags = new LinkedHashMap<>();
    private String defaultSchema = "main";
    private final Map<String, String> schemaComments = new LinkedHashMap<>();
    private final Map<String, Map<String, String>> schemaTags = new LinkedHashMap<>();
    private String implementationVersion;
    private String dataVersionSpec;
    private final List<CatalogDataVersionRelease> releases = new ArrayList<>();
    private String sourceUrl;
    /** Optional 32-byte ChaCha20-Poly1305 key for sealing attach / transaction
     *  opaque data. {@code null} ⇒ per-process random (single-replica only).
     *  See {@link #opaqueDataKey(byte[])}. */
    private byte[] opaqueDataKey;
    /** Supplies the protocols hosted beside {@code vgi.v2}; see {@link #hostedProtocols}. */
    private java.util.function.Supplier<? extends java.util.Collection<HostedProtocol>> hostedProtocolsHook;
    /** {@code vgi_rpc.Identity.v1} hooks; absent unless set. See {@link #resolveToken}. */
    private farm.query.vgirpc.identity.TokenResolveHook resolveTokenHook;
    private farm.query.vgirpc.identity.GrantMintHook mintGrantHook;
    private List<String> introspectPrincipals;
    private Double maxAuthAge;
    /** Sealed-grant keys set in code or by {@code --grant-key}; {@code null} reads the env. */
    private farm.query.vgirpc.identity.GrantKeys grantKeys;
    private final List<ScalarFunction> scalars = new ArrayList<>();
    private final List<TableFunction> tables = new ArrayList<>();
    private final java.util.Set<String> unlistedTables = new java.util.HashSet<>();
    private final List<TableInOutFunction> tableInOuts = new ArrayList<>();
    private final List<farm.query.vgi.buffering.TableBufferingFunction> bufferingFns = new ArrayList<>();
    private final List<AggregateFunction<?>> aggregates = new ArrayList<>();
    private final List<farm.query.vgi.function.FunctionDescriptor> globalFunctions = new ArrayList<>();
    private String globalFunctionPrefix = "";
    private boolean supportsCatalogContents = true;
    private CatalogContentsProvider catalogContentsProvider;
    private CatalogContentsEtag catalogContentsEtag = CatalogContentsEtag.NONE;
    private boolean attachScopedCatalogItems = false;
    private boolean catalogContentsCache = true;
    private final List<SettingSpec> settings = new ArrayList<>();
    private final List<SecretTypeSpec> secretTypes = new ArrayList<>();
    private final List<farm.query.vgi.protocol.AttachCatalogInfo> attachCatalogs = new ArrayList<>();
    private final List<AttachOptionSpec> attachOptions = new ArrayList<>();
    private final List<View> views = new ArrayList<>();

    private final List<Macro> macros = new ArrayList<>();

    /**
     * Register a SQL macro.
     *
     * @param m the macro to register
     * @return this builder
     */
    public Worker registerMacro(Macro m) {
        macros.add(m);
        return this;
    }

    /**
     * Register several SQL macros.
     *
     * @param ms the macros to register
     * @return this builder
     */
    public Worker registerMacros(Iterable<? extends Macro> ms) {
        for (Macro m : ms) macros.add(m);
        return this;
    }

    /**
     * Macros enumerated through {@code catalog_schema_contents_macros}.
     *
     * @return the registered macros, in registration order
     */
    public List<Macro> macros() { return macros; }

    private final List<CatalogTable> catalogTables = new ArrayList<>();
    private final Map<String, List<farm.query.vgi.catalog.ScanBranch>> multiBranchTables =
            new LinkedHashMap<>();
    private final Map<String, List<String>> multiBranchRequiredExtensions =
            new LinkedHashMap<>();

    /**
     * Register a catalog table.
     *
     * @param t the table to register
     * @return this builder
     */
    public Worker registerCatalogTable(CatalogTable t) {
        catalogTables.add(t);
        return this;
    }

    /**
     * Catalog tables enumerated through {@code catalog_schema_contents_tables} /
     * {@code catalog_table_get}.
     *
     * @return the registered catalog tables, in registration order
     */
    public List<CatalogTable> catalogTables() { return catalogTables; }

    /**
     * Register a multi-branch table: a catalog table whose scan is the
     * {@code UNION_ALL} of {@code branches}. The table is enumerated normally;
     * its scan resolves through {@code catalog_table_scan_branches_get}. Pass
     * an empty branch list to exercise the C++ loud-fail path. The stub's
     * inline scan-function (if any) is dropped so the branches RPC drives.
     *
     * @param stub     the catalog table to enumerate (its inline scan is dropped)
     * @param branches the branches whose {@code UNION_ALL} forms the scan; empty exercises the loud-fail path
     * @return this builder
     */
    public Worker registerMultiBranchTable(CatalogTable stub,
            List<farm.query.vgi.catalog.ScanBranch> branches) {
        return registerMultiBranchTable(stub, branches, List.of());
    }

    /**
     * Register a multi-branch table declaring the DuckDB extensions the C++
     * rewriter must auto-load before binding any branch (e.g. {@code "iceberg"}
     * for an {@code iceberg_scan} arm, {@code "parquet"} for {@code read_parquet}
     * where it isn't autoloaded). Surfaced as the {@code required_extensions}
     * field of the {@code catalog_table_scan_branches_get} response.
     *
     * @param stub               the catalog table to enumerate (its inline scan is dropped)
     * @param branches           the branches whose {@code UNION_ALL} forms the scan
     * @param requiredExtensions DuckDB extension names the branches depend on
     * @return this builder
     */
    public Worker registerMultiBranchTable(CatalogTable stub,
            List<farm.query.vgi.catalog.ScanBranch> branches,
            List<String> requiredExtensions) {
        catalogTables.add(stub.withRpcScanFunction());
        String key = stub.schema() + "." + stub.name();
        multiBranchTables.put(key, branches == null ? List.of() : List.copyOf(branches));
        multiBranchRequiredExtensions.put(key,
                requiredExtensions == null ? List.of() : List.copyOf(requiredExtensions));
        return this;
    }

    /**
     * Branches for a multi-branch table.
     *
     * @param schema the schema name
     * @param name   the table name
     * @return the registered branches, or {@code null} if the table is not multi-branch
     */
    public List<farm.query.vgi.catalog.ScanBranch> multiBranchTable(String schema, String name) {
        return multiBranchTables.get(schema + "." + name);
    }

    /**
     * DuckDB extensions the C++ rewriter must auto-load for a multi-branch
     * table's branches, or an empty list when none were declared.
     *
     * @param schema the schema name
     * @param name   the table name
     * @return the required-extension names, never {@code null}
     */
    public List<String> multiBranchRequiredExtensions(String schema, String name) {
        return multiBranchRequiredExtensions.getOrDefault(schema + "." + name, List.of());
    }

    /**
     * Every table registered via
     * {@link #registerMultiBranchTable(CatalogTable, List)}, used by the
     * service to answer {@code catalog_table_scan_branches_get}.
     *
     * @return all multi-branch tables keyed by {@code schema.name}
     */
    public Map<String, List<farm.query.vgi.catalog.ScanBranch>> multiBranchTables() {
        return multiBranchTables;
    }

    private Worker() {}

    /**
     * Start building a worker. Defaults: catalog name {@code "vgi"}, default
     * schema {@code "main"}, empty comment/tags, no versioning metadata.
     *
     * @return a fresh worker builder with default catalog metadata
     */
    public static Worker builder() { return new Worker(); }

    /**
     * Name this worker's catalog. Surfaced as the catalog row in
     * {@code catalog_catalogs()} and as the default database alias on ATTACH.
     *
     * @param name the catalog name (default {@code "vgi"})
     * @return this builder
     */
    public Worker catalogName(String name) { this.catalogName = name; return this; }

    /**
     * Set the catalog-level comment, surfaced through {@code catalog_catalogs()}
     * and DuckDB's {@code duckdb_databases()} comment column.
     *
     * @param comment the catalog comment (default empty)
     * @return this builder
     */
    public Worker catalogComment(String comment) { this.catalogComment = comment; return this; }

    /**
     * Attach key/value tags to the catalog, surfaced through
     * {@code catalog_catalogs()}. Merged into any previously set tags
     * (later calls overwrite duplicate keys).
     *
     * @param tags catalog tag key/value pairs to merge in
     * @return this builder
     */
    public Worker catalogTags(Map<String, String> tags) { this.catalogTags.putAll(tags); return this; }

    /**
     * Advertise the worker's implementation (code) version, reported through
     * {@code catalog_version} alongside the resolved data version so clients
     * can distinguish "what code is running" from "what data it serves".
     *
     * @param v semver implementation version string (e.g. {@code "11.0.0"});
     *          {@code null} (the default) omits it
     * @return this builder
     */
    public Worker implementationVersion(String v) { this.implementationVersion = v; return this; }

    /**
     * Declare the range of data versions this worker can serve. ATTACH-time
     * version requests are validated against this spec; requests outside the
     * range are rejected.
     *
     * @param v semver range spec (e.g. {@code ">=1.0.0,<4.0.0"});
     *          {@code null} (the default) disables version negotiation
     * @return this builder
     */
    public Worker dataVersionSpec(String v) { this.dataVersionSpec = v; return this; }

    /**
     * Implementation version advertised through {@code catalog_version}.
     *
     * @return the configured implementation version, or {@code null} if unset
     */
    public String implementationVersion() { return implementationVersion; }

    /**
     * Data-version range this worker accepts at ATTACH time.
     *
     * @return the configured data-version spec, or {@code null} if unset
     */
    public String dataVersionSpec() { return dataVersionSpec; }

    /**
     * Install the hook that returns additional vgi-rpc protocols to host beside {@code vgi.v2}.
     *
     * <p>Use it to serve other protocols from the same process as the VGI protocol, on the
     * <strong>same</strong> listener whatever the transport. The hook's result is hosted on every
     * transport this worker serves -- stdin/stdout, AF_UNIX, TCP, the Iroh raw upstream and HTTP --
     * after {@code vgi.v2} and in the order returned, so reflection's {@code list_protocols}
     * reports {@code vgi.v2} first and then these.
     *
     * <p>The hook is called <strong>once</strong>, when a transport's server is built. It may
     * consult configuration or the environment, but its answer is fixed for the life of that
     * server, so reflection output and protocol hashes stay stable.
     *
     * <p>The protocol is the unit of optionality: there is no way to host a subset of a protocol's
     * methods. A capability that is optional should be its own protocol, returned here or not.
     *
     * <p>Each protocol needs a distinct wire name ({@code @ProtocolName}). Names may not repeat,
     * may not be {@code vgi.v2}, and may not use the reserved {@code vgi_rpc.} prefix: reflection
     * is hosted automatically, and {@code vgi_rpc.Identity.v1} is enabled by
     * {@link #resolveToken} / {@link #mintGrant}. A violation is a startup error naming this hook.
     * Requests are routed on their {@code vgi_rpc.protocol} key, so hosting more protocols never
     * changes how a {@code vgi.v2} request is dispatched.
     *
     * @param hook returns the pairs to host; {@code null} hosts none
     * @return this worker
     */
    public Worker hostedProtocols(
            java.util.function.Supplier<? extends java.util.Collection<HostedProtocol>> hook) {
        this.hostedProtocolsHook = hook;
        return this;
    }

    /**
     * Host {@code vgi_rpc.Identity.v1}'s {@code introspect_token} on HTTP, backed by {@code hook}.
     *
     * <p>Lets a reverse proxy that terminates the only public listener resolve an opaque bearer
     * credential to a principal. Absent unless set -- not hosted-and-refusing -- which keeps a
     * dependency upgrade from growing a credential-to-identity oracle on every worker. Hosted on
     * HTTP only: the allowlist is a list of principals, which stdin/stdout, AF_UNIX and TCP do not
     * have.
     *
     * <p>The worker's HTTP authentication also consults the hook for bearer credentials its own
     * authenticator does not accept: a resolved credential authenticates as that identity
     * (domain {@code "token"}), {@code null} falls through to 401, and an outage is a 503 with
     * the hook's {@code Retry-After}. JWS-shaped, sealed-grant ({@code vgig1.}) and over-long
     * tokens are never passed to it.
     *
     * <p>Setting this makes an introspector allowlist mandatory -- {@link #introspectPrincipals}
     * or {@code VGI_INTROSPECT_PRINCIPALS} -- and {@link #runHttp} refuses to start without one:
     * "any authenticated caller" lets any user resolve any other user's credential to its owner.
     *
     * <p>The hook returns {@code null} for "the store answered and this credential is unknown".
     * For "the answer is not knowable" (store down, timeout, 5xx), throw
     * {@link farm.query.vgirpc.http.AuthUnavailableException} -- the same error an
     * {@code Authenticator} throws when the same store is down. The framework translates it to
     * {@code identity_unavailable} carrying its retry hint as {@code RetryInfo}, so a caller knows
     * the failure is transient and when to ask again. ({@code IdentityUnavailableError} works
     * too.) Never throw an {@code IllegalArgumentException} or an {@code AuthException} for an
     * outage: those read as a definitive "unknown", which callers may negative-cache.
     *
     * @param hook resolves a credential to its identity
     * @return this worker
     */
    public Worker resolveToken(farm.query.vgirpc.identity.TokenResolveHook hook) {
        this.resolveTokenHook = hook;
        return this;
    }

    /**
     * Host {@code vgi_rpc.Identity.v1}'s {@code issue_grant} on HTTP, backed by {@code hook}.
     *
     * <p>The subject is always the authenticated caller, never a parameter. Throw
     * {@code GrantRefusedError} to decline; for a transient failure throw
     * {@link farm.query.vgirpc.http.AuthUnavailableException}, which reaches the caller as
     * {@code identity_unavailable} with its retry hint. Needs no allowlist: minting is always
     * about the caller.
     *
     * @param hook mints a grant for the calling principal
     * @return this worker
     */
    public Worker mintGrant(farm.query.vgirpc.identity.GrantMintHook hook) {
        this.mintGrantHook = hook;
        return this;
    }

    /**
     * Turn on sealed grants (IDENTITY_V1_SPEC.md §9), overriding {@code VGI_RPC_GRANT_KEYS}.
     *
     * <p>With keys, over HTTP the worker hosts {@code issue_grant} and mints sealed grants itself
     * (unless {@link #mintGrant} supplies a minter of its own), and its HTTP authentication accepts
     * those grants back as bearer credentials: unattended automation presents the grant a user
     * minted and is authenticated as that user, domain {@code "grant"}. A grant-authenticated
     * caller carries no {@code auth_time}, so it cannot mint another grant.
     *
     * <p>Without this the environment decides: {@code VGI_RPC_GRANT_KEYS} (comma-separated
     * base64 keys of 32 bytes, minting key first), {@code VGI_RPC_GRANT_AUDIENCE},
     * {@code VGI_RPC_GRANT_MAX_TTL_SECONDS}, or {@code --grant-key} on the command line. Unset
     * means grants are off and nothing changes. A malformed key stops the worker at startup.
     *
     * @param keys the grant keys, or {@code null} to defer to the environment
     * @return this worker
     */
    public Worker grantKeys(farm.query.vgirpc.identity.GrantKeys keys) {
        this.grantKeys = keys;
        return this;
    }

    /**
     * Principals permitted to call {@code introspect_token}. Overrides
     * {@code VGI_INTROSPECT_PRINCIPALS} (comma-separated). Required whenever
     * {@link #resolveToken} is set; there is no permissive default.
     *
     * @param principals the allowlisted caller principals
     * @return this worker
     */
    public Worker introspectPrincipals(String... principals) {
        this.introspectPrincipals = principals == null ? null : List.of(principals);
        return this;
    }

    /**
     * How recently a caller must have authenticated to mint a grant, in seconds.
     *
     * @param seconds the ceiling on {@code now - auth_time}
     * @return this worker
     */
    public Worker maxAuthAge(double seconds) {
        this.maxAuthAge = seconds;
        return this;
    }

    /**
     * Configure the worker's signing key: the key that seals attach / transaction
     * {@code opaque_data} on HTTP and, together with grants, enables attach tickets
     * ({@code vgi.attach_tickets.v1}, see {@link AttachTickets}). Overrides {@code VGI_SIGNING_KEY},
     * which is read when this is not called.
     *
     * <p>Any length: a 32-byte key is used as-is, any other is replaced by its SHA-256, exactly as
     * {@code VGI_SIGNING_KEY} is normalized in every VGI SDK. A configured key also lets replicas
     * open each other's opaque data and tickets. Without one each HTTP process seals with a random
     * key of its own, and attach tickets are not hosted: every ticket would die on restart.
     *
     * @param key the key bytes, or {@code null} to defer to {@code VGI_SIGNING_KEY}
     * @return this worker
     */
    public Worker signingKey(byte[] key) {
        this.opaqueDataKey = key != null ? AttachTickets.normalizeKey(key) : null;
        return this;
    }

    /**
     * Provide a stable 32-byte key for sealing attach / transaction
     * {@code opaque_data}. Required when running the same worker across
     * multiple HTTP replicas: without it each replica generates its own
     * random key, and a load balancer rotating across them will surface
     * {@code AEADBadTagException} when one replica receives a blob another
     * replica sealed.
     *
     * <p>{@code null} (the default) restores the per-process random-key
     * behaviour, which is correct for single-replica HTTP and irrelevant
     * for stdio / AF_UNIX (where the sealer is disabled entirely).
     *
     * <p>Equivalent to {@link #signingKey(byte[])} with a 32-byte key; a configured key also
     * enables attach tickets when the worker can issue grants.
     *
     * @param key 32-byte ChaCha20-Poly1305 key, or {@code null} for per-process random
     * @return this builder
     * @throws IllegalArgumentException if {@code key} is non-null but not 32 bytes
     */
    public Worker opaqueDataKey(byte[] key) {
        if (key != null && key.length != 32) {
            throw new IllegalArgumentException(
                    "opaqueDataKey must be 32 bytes (got " + key.length + ")");
        }
        this.opaqueDataKey = key != null ? key.clone() : null;
        return this;
    }

    /** Published data-version releases, surfaced through {@code catalog_catalogs()}.
     *  Pass newest-first.
     *
     * @param rs the releases, newest-first
     * @return this builder
     */
    public Worker releases(CatalogDataVersionRelease... rs) {
        for (CatalogDataVersionRelease r : rs) releases.add(r);
        return this;
    }
    /**
     * Data-version releases surfaced through {@code catalog_catalogs()}.
     *
     * @return a copy of the configured releases, in the order passed to
     *         {@link #releases(CatalogDataVersionRelease...)}
     */
    public List<CatalogDataVersionRelease> releases() { return List.copyOf(releases); }

    /**
     * Set the catalog's source URL (e.g. a homepage or repository link),
     * surfaced through {@code catalog_catalogs()}.
     *
     * @param url the source URL; {@code null} (the default) omits it
     * @return this builder
     */
    public Worker sourceUrl(String url) { this.sourceUrl = url; return this; }

    /**
     * Source URL surfaced through {@code catalog_catalogs()}.
     *
     * @return the configured source URL, or {@code null} if unset
     */
    public String sourceUrl() { return sourceUrl; }

    /**
     * Name the schema DuckDB selects by default after ATTACH. Functions,
     * tables and views without an explicit schema register here.
     *
     * @param schema the default schema name (default {@code "main"})
     * @return this builder
     */
    public Worker defaultSchema(String schema) { this.defaultSchema = schema; return this; }

    /** Per-schema comment surfaced via {@code catalog_schemas} /
     *  {@code catalog_schema_get}. Default comment for the default schema is
     *  "Default schema"; any auxiliary schema without an entry gets an empty
     *  comment.
     *
     * @param schema  the schema name
     * @param comment the comment ({@code null} treated as empty)
     * @return this builder
     */
    public Worker schemaComment(String schema, String comment) {
        schemaComments.put(schema, comment == null ? "" : comment);
        return this;
    }

    /**
     * Comments registered via {@link #schemaComment(String, String)}.
     *
     * @return the per-schema comments keyed by schema name
     */
    public Map<String, String> schemaComments() { return schemaComments; }

    /**
     * Attach key/value metadata tags to a schema, surfaced via
     * {@code catalog_schemas} / {@code catalog_schema_get} and reported through
     * DuckDB's {@code duckdb_schemas().tags}. Typical keys are
     * {@code vgi.description_llm} and {@code vgi.description_md}. Merged into any
     * tags previously set for the schema (later calls overwrite duplicate keys).
     *
     * @param schema the schema name
     * @param tags   schema tag key/value pairs to merge in
     * @return this builder
     */
    public Worker schemaTags(String schema, Map<String, String> tags) {
        schemaTags.computeIfAbsent(schema, k -> new LinkedHashMap<>()).putAll(tags);
        return this;
    }

    /**
     * Tags registered via {@link #schemaTags(String, Map)}.
     *
     * @return the per-schema tags keyed by schema name
     */
    public Map<String, Map<String, String>> schemaTags() { return schemaTags; }

    /**
     * An auxiliary catalog served by the same worker process next to the main
     * catalog, MetaWorker-style: it appears as its own row in
     * {@code catalog_catalogs()}, attaches by name with its own versions and a
     * random per-ATTACH opaque id, and owns the functions registered into it
     * through the {@code registerExtraCatalog*} methods (those functions are
     * listed only under this catalog's attaches, and hidden from the main
     * catalog's).
     *
     * @param name the catalog name used in {@code ATTACH '<name>' ...}
     * @param implementationVersion the advertised/resolved implementation version
     * @param dataVersion the advertised {@code data_version_spec} and resolved data version
     * @param schemaComment the comment on the catalog's single {@code main} schema
     * @param attachOptions ATTACH-time options this catalog alone declares, advertised on its
     *                      {@code catalog_catalogs()} row and enforced at its attach. Separate from
     *                      {@link #attachOptions(AttachOptionSpec...)}, which is the main catalog's:
     *                      a worker may serve one catalog that requires an option and another that
     *                      takes none.
     */
    public record ExtraCatalog(String name, String implementationVersion, String dataVersion,
                               String schemaComment, List<AttachOptionSpec> attachOptions,
                               String catalogComment, Map<String, String> schemaComments,
                               boolean versionFrozen, Boolean supportsCatalogContents,
                               CatalogContentsProvider contentsProvider, CatalogContentsEtag contentsEtag) {

        /** Defensive copies; a null option list reads as none declared, a null etag policy as NONE. */
        public ExtraCatalog {
            attachOptions = attachOptions == null ? List.of() : List.copyOf(attachOptions);
            schemaComments = schemaComments == null ? Map.of() : Map.copyOf(schemaComments);
            contentsEtag = contentsEtag == null ? CatalogContentsEtag.NONE : contentsEtag;
        }

        /**
         * An auxiliary catalog with attach options and the defaults for everything
         * added since: no catalog comment, version not frozen, {@code catalog_contents}
         * served as the worker's own catalog serves it.
         *
         * @param name the catalog name used in {@code ATTACH '<name>' ...}
         * @param implementationVersion the advertised/resolved implementation version
         * @param dataVersion the advertised {@code data_version_spec} and resolved data version
         * @param schemaComment the comment on the catalog's {@code main} schema
         * @param attachOptions ATTACH-time options this catalog alone declares
         */
        public ExtraCatalog(String name, String implementationVersion, String dataVersion,
                            String schemaComment, List<AttachOptionSpec> attachOptions) {
            this(name, implementationVersion, dataVersion, schemaComment, attachOptions,
                    null, Map.of(), false, null, null, null);
        }

        /**
         * An auxiliary catalog declaring no attach options of its own.
         *
         * @param name the catalog name used in {@code ATTACH '<name>' ...}
         * @param implementationVersion the advertised/resolved implementation version
         * @param dataVersion the advertised {@code data_version_spec} and resolved data version
         * @param schemaComment the comment on the catalog's single {@code main} schema
         */
        public ExtraCatalog(String name, String implementationVersion, String dataVersion,
                            String schemaComment) {
            this(name, implementationVersion, dataVersion, schemaComment, List.of());
        }

        /**
         * This catalog with a catalog-level comment (the attach result's {@code comment}).
         *
         * @param comment the comment, or {@code null}
         * @return a copy
         */
        public ExtraCatalog withCatalogComment(String comment) {
            return new ExtraCatalog(name, implementationVersion, dataVersion, schemaComment, attachOptions,
                    comment, schemaComments, versionFrozen, supportsCatalogContents, contentsProvider,
                    contentsEtag);
        }

        /**
         * This catalog with a comment on one of its schemas other than {@code main}
         * ({@code main}'s is {@link #schemaComment()}).
         *
         * @param schema  the schema name
         * @param comment the comment
         * @return a copy
         */
        public ExtraCatalog withSchemaComment(String schema, String comment) {
            Map<String, String> comments = new LinkedHashMap<>(schemaComments);
            comments.put(schema, comment);
            return new ExtraCatalog(name, implementationVersion, dataVersion, schemaComment, attachOptions,
                    catalogComment, comments, versionFrozen, supportsCatalogContents, contentsProvider,
                    contentsEtag);
        }

        /**
         * This catalog with {@code catalog_version_frozen} set: a static catalog whose
         * version never changes, so the client never re-checks it.
         *
         * @param frozen whether the version is frozen
         * @return a copy
         */
        public ExtraCatalog withVersionFrozen(boolean frozen) {
            return new ExtraCatalog(name, implementationVersion, dataVersion, schemaComment, attachOptions,
                    catalogComment, schemaComments, frozen, supportsCatalogContents, contentsProvider,
                    contentsEtag);
        }

        /**
         * This catalog with its own {@code catalog_contents} behaviour, instead of
         * inheriting the worker's ({@link Worker#supportsCatalogContents(boolean)},
         * {@link Worker#catalogContents(CatalogContentsProvider)},
         * {@link Worker#catalogContentsEtag(CatalogContentsEtag)}).
         *
         * @param supports whether {@code catalog_attach} advertises {@code supports_catalog_contents}
         * @param provider the catalog's own answer, or {@code null} for the default snapshot
         * @param etag     the framework etag policy, or {@code null} for {@link CatalogContentsEtag#NONE}
         * @return a copy
         */
        public ExtraCatalog withCatalogContents(boolean supports, CatalogContentsProvider provider,
                                                CatalogContentsEtag etag) {
            return new ExtraCatalog(name, implementationVersion, dataVersion, schemaComment, attachOptions,
                    catalogComment, schemaComments, versionFrozen, supports, provider,
                    etag == null ? CatalogContentsEtag.NONE : etag);
        }

        /**
         * Whether this catalog's {@code catalog_contents} is configured on the catalog
         * itself ({@link #withCatalogContents}) rather than inherited from the worker.
         *
         * @return {@code true} once {@link #withCatalogContents} was applied
         */
        public boolean ownsCatalogContents() { return supportsCatalogContents != null; }
    }

    private final Map<String, ExtraCatalog> extraCatalogs = new LinkedHashMap<>();

    /**
     * Register an auxiliary catalog served next to the main one.
     *
     * @param catalog the catalog descriptor
     * @return this builder
     */
    public Worker registerExtraCatalog(ExtraCatalog catalog) {
        extraCatalogs.put(catalog.name(), catalog);
        return this;
    }

    /**
     * The auxiliary catalogs registered via {@link #registerExtraCatalog}.
     *
     * @return the catalogs keyed by name, in registration order
     */
    public Map<String, ExtraCatalog> extraCatalogs() { return extraCatalogs; }

    private final Map<String, java.util.function.Function<farm.query.vgi.protocol.CatalogAttachRequest, byte[]>>
            extraCatalogAttachData = new LinkedHashMap<>();

    /**
     * Let an auxiliary catalog derive its own attach bytes from the attach request -- the Java
     * counterpart of a vgi-python catalog returning its own {@code attach_opaque_data}.
     *
     * <p>At {@code catalog_attach} the hook receives the request (options included, secret ones
     * too) and returns bytes the framework appends to a fresh random id: the catalog's functions
     * then see {@code uuid(16) || bytes} as their attach id. The value is sealed on HTTP but
     * travels in plaintext on stdio / AF_UNIX, so the hook MUST NOT return a secret option: derive
     * what the catalog needs from it instead (a digest, a region, a handle) --
     * {@code docs/protocol/vgi-opaque-data-sealing.md} rule 5. Throw to refuse the attach.
     *
     * @param catalogName the auxiliary catalog
     * @param derive maps the attach request to the catalog's own bytes
     * @return this worker
     */
    public Worker extraCatalogAttachData(String catalogName,
            java.util.function.Function<farm.query.vgi.protocol.CatalogAttachRequest, byte[]> derive) {
        extraCatalogAttachData.put(catalogName, derive);
        return this;
    }

    /**
     * The attach-bytes hook of an auxiliary catalog, or {@code null}; see
     * {@link #extraCatalogAttachData(String, java.util.function.Function)}.
     *
     * @param catalogName the auxiliary catalog
     * @return the hook, or {@code null}
     */
    public java.util.function.Function<farm.query.vgi.protocol.CatalogAttachRequest, byte[]> extraCatalogAttachData(
            String catalogName) {
        return extraCatalogAttachData.get(catalogName);
    }

    private final Map<String, List<CatalogTable>> extraCatalogTables = new LinkedHashMap<>();

    /**
     * Register a catalog table owned by an auxiliary catalog. Such tables are
     * enumerated only under that catalog's attaches (and never appear in the
     * main catalog's listings). The scan functions they reference should be
     * registered through {@link #registerExtraCatalogTableFunction} into the
     * same catalog so they are likewise owned by it.
     *
     * @param catalogName the owning auxiliary catalog name
     * @param t the catalog table
     * @return this builder
     */
    public Worker registerExtraCatalogTable(String catalogName, CatalogTable t) {
        extraCatalogTables.computeIfAbsent(catalogName, k -> new ArrayList<>()).add(t);
        return this;
    }

    /**
     * Catalog tables owned by auxiliary catalogs, keyed by catalog name.
     *
     * @return the extra-catalog tables keyed by catalog name
     */
    public Map<String, List<CatalogTable>> extraCatalogTables() { return extraCatalogTables; }

    private final Map<String, List<View>> extraCatalogViews = new LinkedHashMap<>();
    private final Map<String, List<Macro>> extraCatalogMacros = new LinkedHashMap<>();

    /**
     * Register a view owned by an auxiliary catalog, in the schema the view
     * names. Listed only under that catalog's attaches.
     *
     * @param catalogName the owning auxiliary catalog name
     * @param v the view
     * @return this builder
     */
    public Worker registerExtraCatalogView(String catalogName, View v) {
        extraCatalogViews.computeIfAbsent(catalogName, k -> new ArrayList<>()).add(v);
        return this;
    }

    /**
     * Views owned by auxiliary catalogs, keyed by catalog name.
     *
     * @return the extra-catalog views keyed by catalog name
     */
    public Map<String, List<View>> extraCatalogViews() { return extraCatalogViews; }

    /**
     * Register a macro owned by an auxiliary catalog, in the schema the macro
     * names. Listed only under that catalog's attaches.
     *
     * @param catalogName the owning auxiliary catalog name
     * @param m the macro
     * @return this builder
     */
    public Worker registerExtraCatalogMacro(String catalogName, Macro m) {
        extraCatalogMacros.computeIfAbsent(catalogName, k -> new ArrayList<>()).add(m);
        return this;
    }

    /**
     * Macros owned by auxiliary catalogs, keyed by catalog name.
     *
     * @return the extra-catalog macros keyed by catalog name
     */
    public Map<String, List<Macro>> extraCatalogMacros() { return extraCatalogMacros; }

    private final Map<String, CatalogInterface> catalogInterfaces = new LinkedHashMap<>();

    /**
     * Serve a catalog implemented in code next to this worker's own (MetaWorker-style):
     * it gets its own {@code catalog_catalogs()} row, and every catalog RPC for one of
     * its attaches is routed to it. See {@link CatalogInterface}.
     *
     * @param catalog the catalog
     * @return this builder
     * @throws IllegalArgumentException if the name is already served by this worker
     */
    public Worker registerCatalog(CatalogInterface catalog) {
        String name = catalog.name();
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("catalog name is empty");
        if (name.equals(catalogName) || extraCatalogs.containsKey(name) || catalogInterfaces.containsKey(name)) {
            throw new IllegalArgumentException("catalog " + name + " is already served by this worker");
        }
        catalogInterfaces.put(name, catalog);
        return this;
    }

    /**
     * The catalogs registered via {@link #registerCatalog(CatalogInterface)}.
     *
     * @return the catalogs keyed by name, in registration order
     */
    public Map<String, CatalogInterface> catalogInterfaces() { return catalogInterfaces; }

    /**
     * Where a registered function is declared: the catalog that owns it
     * ({@code null} = this worker's own catalog) and the schema inside it.
     *
     * @param catalogName the owning auxiliary catalog, or {@code null} for the main catalog
     * @param schemaName  the owning schema
     */
    private record FunctionHome(String catalogName, List<String> schemaPath) {}

    /**
     * Explicit (catalog, schema) placement per registered function instance.
     * Keyed by identity: two distinct instances may share a registered name —
     * that is exactly the collision schema-scoped dispatch exists to break.
     * Functions absent from this map live in {@link #defaultSchema()} of the
     * main catalog, which is where DuckDB registers them.
     */
    private final Map<Object, FunctionHome> functionHomes = new java.util.IdentityHashMap<>();

    private Worker home(Object fn, String catalogName, List<String> schemaPath) {
        functionHomes.put(fn, new FunctionHome(catalogName, List.copyOf(schemaPath)));
        return this;
    }

    /**
     * The catalog schema {@code fn} is declared in — the schema DuckDB
     * registers it into and therefore the one a bind request names. Every
     * registered function has exactly one: a registration that names no schema
     * resolves to {@link #defaultSchema()}, which is a real home, not a
     * wildcard. Nothing is visible in more than one schema.
     *
     * @param fn a registered function instance
     * @return the owning schema name, never {@code null}
     */
    public List<String> schemaPathOf(Object fn) {
        FunctionHome h = functionHomes.get(fn);
        return h == null || h.schemaPath() == null ? List.of(defaultSchema) : h.schemaPath();
    }

    /** Legacy single-component view of a function's schema path. */
    public String schemaOf(Object fn) {
        List<String> path = schemaPathOf(fn);
        return path.isEmpty() ? "" : path.get(path.size() - 1);
    }

    /**
     * Resolve the schema containing a named table function. The table's schema
     * wins when that function is registered there; otherwise a single
     * unambiguous registration is returned. Native DuckDB functions and
     * ambiguous names return {@code null}.
     *
     * @param functionName the function named by a scan result
     * @param tableSchemaPath the schema path containing the table being resolved
     * @param catalogName the auxiliary catalog owner, or {@code null} for this worker
     * @return the authoritative function schema, or {@code null}
     */
    public List<String> resolveTableFunctionSchemaPath(
            String functionName, List<String> tableSchemaPath, String catalogName) {
        java.util.LinkedHashSet<List<String>> homes = new java.util.LinkedHashSet<>();
        for (TableFunction fn : tables) {
            if (!fn.name().equals(functionName)
                    || !java.util.Objects.equals(catalogOf(fn), catalogName)) continue;
            List<String> schema = schemaPathOf(fn);
            if (schema.equals(tableSchemaPath)) return schema;
            homes.add(schema);
        }
        return homes.size() == 1 ? homes.iterator().next() : null;
    }

    /** Compatibility helper for single-component schema callers. */
    public String resolveTableFunctionSchema(
            String functionName, String tableSchema, String catalogName) {
        List<String> result = resolveTableFunctionSchemaPath(
                functionName, List.of(tableSchema), catalogName);
        return result == null || result.isEmpty() ? null : result.get(result.size() - 1);
    }

    /**
     * The auxiliary catalog {@code fn} is declared in, or {@code null} when it
     * belongs to this worker's own catalog. Ownership is always explicit — a
     * function is registered into exactly one catalog — so two auxiliary
     * catalogs may declare the very same function name and still dispatch
     * apart.
     *
     * @param fn a registered function instance
     * @return the owning auxiliary catalog name, or {@code null} for the main catalog
     */
    public String catalogOf(Object fn) {
        FunctionHome h = functionHomes.get(fn);
        return h == null ? null : h.catalogName();
    }

    /**
     * Register a scalar function, callable from SQL and enumerated through
     * {@code catalog_schema_contents_functions}.
     *
     * @param fn the scalar function to register
     * @return this builder
     */
    public Worker registerScalar(ScalarFunction fn) {
        scalars.add(fn);
        return this;
    }

    /**
     * Register a scalar function into a named schema of this worker's catalog
     * (rather than {@link #defaultSchema()}). The same function name may be
     * registered in more than one schema: DuckDB registers one entry per
     * schema, and bind requests carry the schema so each call reaches the
     * implementation the caller named.
     *
     * @param schemaName the schema to declare the function in
     * @param fn the scalar function to register
     * @return this builder
     */
    public Worker registerScalar(String schemaName, ScalarFunction fn) {
        return registerScalar(List.of(schemaName), fn);
    }

    public Worker registerScalar(List<String> schemaPath, ScalarFunction fn) {
        scalars.add(fn);
        return home(fn, null, schemaPath);
    }

    /**
     * Register a table function into a named schema of this worker's catalog
     * (rather than {@link #defaultSchema()}). See
     * {@link #registerScalar(String, ScalarFunction)}.
     *
     * @param schemaName the schema to declare the function in
     * @param fn the table function to register
     * @return this builder
     */
    public Worker registerTable(String schemaName, TableFunction fn) {
        return registerTable(List.of(schemaName), fn);
    }

    public Worker registerTable(List<String> schemaPath, TableFunction fn) {
        tables.add(fn);
        return home(fn, null, schemaPath);
    }

    /**
     * Register a table-in-out function into a named schema of this worker's
     * catalog (rather than {@link #defaultSchema()}). See
     * {@link #registerScalar(String, ScalarFunction)}.
     *
     * @param schemaName the schema to declare the function in
     * @param fn the table-in-out function to register
     * @return this builder
     */
    public Worker registerTableInOut(String schemaName, TableInOutFunction fn) {
        return registerTableInOut(List.of(schemaName), fn);
    }

    public Worker registerTableInOut(List<String> schemaPath, TableInOutFunction fn) {
        tableInOuts.add(fn);
        return home(fn, null, schemaPath);
    }

    /**
     * Register a table-buffering function into a named schema of this worker's
     * catalog (rather than {@link #defaultSchema()}). See
     * {@link #registerScalar(String, ScalarFunction)}.
     *
     * @param schemaName the schema to declare the function in
     * @param fn the table-buffering function to register
     * @return this builder
     */
    public Worker registerTableBuffering(String schemaName,
            farm.query.vgi.buffering.TableBufferingFunction fn) {
        return registerTableBuffering(List.of(schemaName), fn);
    }

    public Worker registerTableBuffering(List<String> schemaPath,
            farm.query.vgi.buffering.TableBufferingFunction fn) {
        bufferingFns.add(fn);
        return home(fn, null, schemaPath);
    }

    /**
     * Register an aggregate function into a named schema of this worker's
     * catalog (rather than {@link #defaultSchema()}). See
     * {@link #registerScalar(String, ScalarFunction)}.
     *
     * @param schemaName the schema to declare the function in
     * @param fn the aggregate function to register
     * @return this builder
     */
    public Worker registerAggregate(String schemaName, AggregateFunction<?> fn) {
        return registerAggregate(List.of(schemaName), fn);
    }

    public Worker registerAggregate(List<String> schemaPath, AggregateFunction<?> fn) {
        aggregates.add(fn);
        return home(fn, null, schemaPath);
    }

    /**
     * Register a scalar function owned by an auxiliary catalog, in a named
     * schema of it. The function is listed only under that catalog's attaches
     * and hidden from the main catalog's. Ownership is explicit per function,
     * so two auxiliary catalogs can declare the SAME function name and still
     * dispatch apart — the attach names the catalog.
     *
     * @param catalogName the owning auxiliary catalog (see {@link #registerExtraCatalog})
     * @param schemaName the schema inside that catalog
     * @param fn the scalar function to register
     * @return this builder
     */
    public Worker registerExtraCatalogScalar(String catalogName, String schemaName, ScalarFunction fn) {
        return registerExtraCatalogScalar(catalogName, List.of(schemaName), fn);
    }

    public Worker registerExtraCatalogScalar(String catalogName, List<String> schemaPath, ScalarFunction fn) {
        scalars.add(fn);
        return home(fn, catalogName, schemaPath);
    }

    /**
     * Register a table function owned by an auxiliary catalog, in a named
     * schema of it. See {@link #registerExtraCatalogScalar}.
     *
     * @param catalogName the owning auxiliary catalog (see {@link #registerExtraCatalog})
     * @param schemaName the schema inside that catalog
     * @param fn the table function to register
     * @return this builder
     */
    public Worker registerExtraCatalogTableFunction(String catalogName, String schemaName, TableFunction fn) {
        return registerExtraCatalogTableFunction(catalogName, List.of(schemaName), fn);
    }

    public Worker registerExtraCatalogTableFunction(String catalogName, List<String> schemaPath, TableFunction fn) {
        tables.add(fn);
        return home(fn, catalogName, schemaPath);
    }

    /**
     * Register a table-in-out function owned by an auxiliary catalog, in a
     * named schema of it. See {@link #registerExtraCatalogScalar}.
     *
     * @param catalogName the owning auxiliary catalog (see {@link #registerExtraCatalog})
     * @param schemaName the schema inside that catalog
     * @param fn the table-in-out function to register
     * @return this builder
     */
    public Worker registerExtraCatalogTableInOut(String catalogName, String schemaName, TableInOutFunction fn) {
        return registerExtraCatalogTableInOut(catalogName, List.of(schemaName), fn);
    }

    public Worker registerExtraCatalogTableInOut(String catalogName, List<String> schemaPath, TableInOutFunction fn) {
        tableInOuts.add(fn);
        return home(fn, catalogName, schemaPath);
    }

    /**
     * Register an aggregate function owned by an auxiliary catalog, in a named
     * schema of it. See {@link #registerExtraCatalogScalar}.
     *
     * @param catalogName the owning auxiliary catalog (see {@link #registerExtraCatalog})
     * @param schemaName  the schema within that catalog
     * @param fn          the aggregate function
     * @return this builder
     */
    public Worker registerExtraCatalogAggregate(String catalogName, String schemaName, AggregateFunction<?> fn) {
        aggregates.add(fn);
        return home(fn, catalogName, List.of(schemaName));
    }

    /**
     * Register a table-buffering function owned by an auxiliary catalog, in a
     * named schema of it. See {@link #registerExtraCatalogScalar}.
     *
     * @param catalogName the owning auxiliary catalog (see {@link #registerExtraCatalog})
     * @param schemaName the schema inside that catalog
     * @param fn the table-buffering function to register
     * @return this builder
     */
    public Worker registerExtraCatalogTableBuffering(String catalogName, String schemaName,
            farm.query.vgi.buffering.TableBufferingFunction fn) {
        return registerExtraCatalogTableBuffering(catalogName, List.of(schemaName), fn);
    }

    public Worker registerExtraCatalogTableBuffering(String catalogName, List<String> schemaPath,
            farm.query.vgi.buffering.TableBufferingFunction fn) {
        bufferingFns.add(fn);
        return home(fn, catalogName, schemaPath);
    }

    /**
     * Register a table function, callable from SQL and enumerated through
     * {@code catalog_schema_contents_functions}.
     *
     * @param fn the table function to register
     * @return this builder
     */
    public Worker registerTable(TableFunction fn) {
        tables.add(fn);
        return this;
    }

    /**
     * Register an aggregate function, callable from SQL and enumerated
     * through {@code catalog_schema_contents_functions}.
     *
     * @param fn the aggregate function to register
     * @return this builder
     */
    public Worker registerAggregate(AggregateFunction<?> fn) {
        aggregates.add(fn);
        return this;
    }

    /**
     * Register a table-in-out function (consumes an input relation, streams
     * an output relation), enumerated through
     * {@code catalog_schema_contents_functions}.
     *
     * @param fn the table-in-out function to register
     * @return this builder
     */
    public Worker registerTableInOut(TableInOutFunction fn) {
        tableInOuts.add(fn);
        return this;
    }

    /**
     * Register several scalar functions; equivalent to calling
     * {@link #registerScalar(ScalarFunction)} for each.
     *
     * @param fns the scalar functions to register
     * @return this builder
     */
    public Worker registerScalars(Iterable<? extends ScalarFunction> fns) {
        for (ScalarFunction f : fns) scalars.add(f);
        return this;
    }

    /**
     * Register several table functions; equivalent to calling
     * {@link #registerTable(TableFunction)} for each.
     *
     * @param fns the table functions to register
     * @return this builder
     */
    public Worker registerTables(Iterable<? extends TableFunction> fns) {
        for (TableFunction f : fns) tables.add(f);
        return this;
    }

    /**
     * Register a table function that is dispatchable but <em>not</em> advertised
     * in the catalog's function listing, so DuckDB never registers it as a
     * callable table function. Use this for the scan function behind a
     * function-backed {@link farm.query.vgi.catalog.CatalogTable} that should
     * surface only as a table (mirrors vgi-python, where a {@code Table(function=F)}
     * does not imply {@code F} is in the catalog's {@code functions} list).
     *
     * @param fn the table function to register for dispatch only
     * @return this builder
     */
    public Worker registerUnlistedTable(TableFunction fn) {
        tables.add(fn);
        unlistedTables.add(fn.name());
        return this;
    }

    /**
     * Names registered via {@link #registerUnlistedTable}: dispatchable, but
     * omitted from {@code catalog_schema_contents_functions}.
     *
     * @return the unlisted table-function names
     */
    public java.util.Set<String> unlistedTables() { return unlistedTables; }

    /**
     * Register several aggregate functions; equivalent to calling
     * {@link #registerAggregate(AggregateFunction)} for each.
     *
     * @param fns the aggregate functions to register
     * @return this builder
     */
    public Worker registerAggregates(Iterable<? extends AggregateFunction<?>> fns) {
        for (AggregateFunction<?> f : fns) aggregates.add(f);
        return this;
    }

    /**
     * Register a table-buffering (Sink+Source) function: DuckDB sinks the
     * full input through {@code table_buffering_process}/{@code _combine}
     * before the finalize stream sources results back out.
     *
     * @param fn the table-buffering function to register
     * @return this builder
     */
    public Worker registerTableBuffering(farm.query.vgi.buffering.TableBufferingFunction fn) {
        bufferingFns.add(fn);
        return this;
    }

    /**
     * Register several table-buffering functions; equivalent to calling
     * {@link #registerTableBuffering} for each.
     *
     * @param fns the table-buffering functions to register
     * @return this builder
     */
    public Worker registerTableBufferings(Iterable<? extends farm.query.vgi.buffering.TableBufferingFunction> fns) {
        for (var f : fns) bufferingFns.add(f);
        return this;
    }

    /**
     * Table-buffering functions registered via {@link #registerTableBuffering}.
     *
     * @return the registered table-buffering functions, in registration order
     */
    public List<farm.query.vgi.buffering.TableBufferingFunction> bufferingFunctions() {
        return bufferingFns;
    }

    /**
     * Register several table-in-out functions; equivalent to calling
     * {@link #registerTableInOut(TableInOutFunction)} for each.
     *
     * @param fns the table-in-out functions to register
     * @return this builder
     */
    public Worker registerTableInOuts(Iterable<? extends TableInOutFunction> fns) {
        for (TableInOutFunction f : fns) tableInOuts.add(f);
        return this;
    }

    /**
     * Ask the client to publish these already-registered functions into its
     * <em>global</em> (non-catalog) function namespace, under
     * {@link #globalFunctionPrefix(String)}. They are advertised on the
     * {@code catalog_attach} result's {@code global_functions} field as
     * serialized {@code FunctionInfo} records (protocol 1.3.0).
     *
     * <p>Each argument must be the <em>same instance</em> passed to a
     * {@code register*} method: the advertised {@code FunctionInfo} carries the
     * schema the function is homed in, which is the bind-dispatch key, and
     * instance identity is what {@link #schemaOf(Object)} resolves.
     * Registration into the catalog is unchanged — publication is additive.
     *
     * @param fns the registered functions to publish globally
     * @return this builder
     */
    public Worker registerGlobalFunctions(Iterable<? extends farm.query.vgi.function.FunctionDescriptor> fns) {
        for (farm.query.vgi.function.FunctionDescriptor f : fns) globalFunctions.add(f);
        return this;
    }

    /**
     * Functions advertised via {@link #registerGlobalFunctions}.
     *
     * @return the functions to publish globally, in declaration order
     */
    public List<farm.query.vgi.function.FunctionDescriptor> globalFunctions() { return globalFunctions; }

    /**
     * Prefix the client applies to every {@link #registerGlobalFunctions} entry
     * to form its globally visible name — e.g. {@code "vgi_example"} publishes
     * {@code global_scalar} as {@code vgi_example_global_scalar}. An empty
     * prefix publishes bare names.
     *
     * @param prefix the global-name prefix
     * @return this builder
     */
    public Worker globalFunctionPrefix(String prefix) {
        globalFunctionPrefix = prefix == null ? "" : prefix;
        return this;
    }

    /**
     * The prefix set by {@link #globalFunctionPrefix(String)}.
     *
     * @return the global-name prefix; empty when unset
     */
    public String globalFunctionPrefix() { return globalFunctionPrefix; }

    /**
     * Whether {@code catalog_attach} advertises {@code supports_catalog_contents},
     * letting the client load the whole catalog with one {@code catalog_contents}
     * call instead of {@code catalog_schemas} plus a
     * {@code catalog_schema_contents_*} call per schema and kind.
     *
     * <p>On by default: a {@code Worker} catalog is declarative and read-only,
     * which is the case vgi-python's {@code ReadOnlyCatalogInterface} advertises
     * it for. The RPC is served either way ({@link VgiService#catalog_contents}
     * composes the per-schema RPCs); this only decides whether the client is told
     * to use it. Turn it off for a catalog whose contents depend on the
     * transaction, since the bulk answer is cached for the whole attach.</p>
     *
     * @param enabled whether to advertise {@code catalog_contents}
     * @return this builder
     */
    public Worker supportsCatalogContents(boolean enabled) {
        supportsCatalogContents = enabled;
        return this;
    }

    /**
     * Whether {@code catalog_attach} advertises {@code supports_catalog_contents}.
     *
     * @return the flag set by {@link #supportsCatalogContents(boolean)}; {@code true} by default
     */
    public boolean supportsCatalogContents() { return supportsCatalogContents; }

    /**
     * Install the catalog's own {@code catalog_contents} answer: it receives the
     * client's {@code if_none_match} and returns the snapshot with an etag, or
     * {@code not_modified} — so a cheap validator can short-circuit before
     * anything is built. Without one, the worker serves the default snapshot
     * (its per-schema RPCs composed) with no etag.
     * {@link CatalogContentsProvider#versionEtag()} is a ready-made validator.
     *
     * <p>The answer goes through {@link CatalogContents#respond}: a
     * {@code not_modified} must carry the etag equal to {@code if_none_match}
     * and no schemas, schema paths must be unique with every parent present,
     * and a full answer whose etag matches {@code if_none_match} is sent as
     * {@code not_modified}.</p>
     *
     * @param provider the provider, or {@code null} for the default
     * @return this builder
     */
    public Worker catalogContents(CatalogContentsProvider provider) {
        catalogContentsProvider = provider;
        return this;
    }

    /**
     * The catalog's own {@code catalog_contents} answer.
     *
     * @return the provider set by {@link #catalogContents(CatalogContentsProvider)}, or {@code null}
     */
    public CatalogContentsProvider catalogContentsProvider() { return catalogContentsProvider; }

    /**
     * The framework's etag policy for {@code catalog_contents}.
     * {@link CatalogContentsEtag#CONTENT_HASH}: when the catalog returns no etag
     * of its own, the etag is the SHA-256 of the snapshot and a matching
     * {@code if_none_match} becomes {@code not_modified}. Default
     * {@link CatalogContentsEtag#NONE}.
     *
     * @param mode the policy
     * @return this builder
     */
    public Worker catalogContentsEtag(CatalogContentsEtag mode) {
        catalogContentsEtag = mode == null ? CatalogContentsEtag.NONE : mode;
        return this;
    }

    /**
     * The framework's etag policy for {@code catalog_contents}.
     *
     * @return the policy set by {@link #catalogContentsEtag(CatalogContentsEtag)}
     */
    public CatalogContentsEtag catalogContentsEtag() { return catalogContentsEtag; }

    /**
     * Whether catalog items embed the per-attach {@code attach_opaque_data}.
     *
     * <p>Off by default: a {@code Worker} catalog is declarative, so its items
     * ({@code SchemaInfo.attach_opaque_data}) carry a fixed value
     * ({@code VgiServiceImpl.FIXED_ITEM_ATTACH_ID}, the same bytes vgi-python's
     * {@code ReadOnlyCatalogInterface} uses) and are identical for every
     * attach. The client keeps and resends the per-attach envelope from
     * {@code catalog_attach} either way, so auth, routing (extra catalogs) and
     * data-version scoping are unaffected. Turn it on only for a catalog whose
     * items genuinely need per-attach scoping; it also disables the
     * {@code catalog_contents} cache ({@link #catalogContentsCache(boolean)}).</p>
     *
     * @param enabled whether to embed the per-attach id in catalog items
     * @return this builder
     */
    public Worker attachScopedCatalogItems(boolean enabled) {
        attachScopedCatalogItems = enabled;
        return this;
    }

    /**
     * Whether catalog items embed the per-attach {@code attach_opaque_data}.
     *
     * @return the flag set by {@link #attachScopedCatalogItems(boolean)}; {@code false} by default
     */
    public boolean attachScopedCatalogItems() { return attachScopedCatalogItems; }

    /**
     * Whether the worker caches its {@code catalog_contents} answer.
     *
     * <p>On by default. A {@code Worker} catalog never changes its version and
     * (unless {@link #attachScopedCatalogItems(boolean)}) its items do not
     * depend on the attach, so the answer depends only on the attached catalog
     * (the worker's own or an extra catalog), the attach's resolved data
     * version, and the catalog version. It is built once per such key — the
     * {@link #catalogContents(CatalogContentsProvider) provider} is asked once,
     * with no {@code if_none_match} — and every later call reuses it, answering
     * a matching {@code if_none_match} with {@code not_modified} from the cached
     * etag. Turn it off for a provider whose answer depends on anything else.</p>
     *
     * @param enabled whether to cache the {@code catalog_contents} answer
     * @return this builder
     */
    public Worker catalogContentsCache(boolean enabled) {
        catalogContentsCache = enabled;
        return this;
    }

    /**
     * Whether the {@code catalog_contents} answer is cached.
     *
     * @return {@code true} when the cache is on and items are attach-independent
     */
    public boolean catalogContentsCache() { return catalogContentsCache && !attachScopedCatalogItems; }

    /**
     * Advertise custom session settings in the {@code catalog_attach} result.
     * DuckDB registers each as a {@code SET}-able option whose current value
     * is forwarded to the worker on every bind.
     *
     * @param specs the setting specs to advertise
     * @return this builder
     */
    public Worker settings(SettingSpec... specs) {
        for (SettingSpec s : specs) settings.add(s);
        return this;
    }

    /**
     * Advertise secret types in the {@code catalog_attach} result. DuckDB
     * registers each so {@code CREATE SECRET} of that type resolves against
     * this catalog, and matching secrets flow to the worker on bind.
     *
     * @param specs the secret-type specs to advertise
     * @return this builder
     */
    public Worker secretTypes(SecretTypeSpec... specs) {
        for (SecretTypeSpec s : specs) secretTypes.add(s);
        return this;
    }

    /**
     * Secret types advertised at attach time.
     *
     * @return the registered secret-type specs, in registration order
     */
    public List<SecretTypeSpec> secretTypeSpecs() { return secretTypes; }

    /**
     * Advertise companion catalogs (lakehouse federation) that the client should
     * ATTACH when this VGI catalog attaches. Surfaced via
     * {@code catalog_attach.attach_catalogs}; the C++ extension attaches each at
     * VGI-attach time so multi-branch catalog-table branches can resolve them.
     *
     * @param catalogs the companion catalogs to advertise
     * @return this builder
     */
    public Worker attachCatalogs(farm.query.vgi.protocol.AttachCatalogInfo... catalogs) {
        for (farm.query.vgi.protocol.AttachCatalogInfo c : catalogs) attachCatalogs.add(c);
        return this;
    }

    /**
     * Companion catalogs advertised at attach time.
     *
     * @return the registered companion catalogs, in registration order
     */
    public List<farm.query.vgi.protocol.AttachCatalogInfo> attachCatalogInfos() { return attachCatalogs; }

    /**
     * Declare the options this worker accepts in DuckDB's
     * {@code ATTACH ... (key value, ...)} clause. Unknown options are
     * rejected client-side; accepted values arrive in the attach request.
     *
     * @param specs the ATTACH-time option specs to accept
     * @return this builder
     */
    public Worker attachOptions(AttachOptionSpec... specs) {
        for (AttachOptionSpec s : specs) attachOptions.add(s);
        return this;
    }

    /**
     * ATTACH-time options declared via {@link #attachOptions(AttachOptionSpec...)}.
     *
     * @return a copy of the registered attach-option specs
     */
    public List<AttachOptionSpec> attachOptionSpecs() { return List.copyOf(attachOptions); }

    /**
     * Register a view, enumerated through {@code catalog_schema_contents_views};
     * its SQL text is expanded by DuckDB at query time.
     *
     * @param v the view to register
     * @return this builder
     */
    public Worker registerView(View v) {
        views.add(v);
        return this;
    }

    /**
     * Register several views; equivalent to calling
     * {@link #registerView(View)} for each.
     *
     * @param vs the views to register
     * @return this builder
     */
    public Worker registerViews(Iterable<? extends View> vs) {
        for (View v : vs) views.add(v);
        return this;
    }

    /**
     * Views enumerated through {@code catalog_schema_contents_views}.
     *
     * @return the registered views, in registration order
     */
    public List<View> views() { return views; }

    /**
     * Register several catalog tables; equivalent to calling
     * {@link #registerCatalogTable(CatalogTable)} for each.
     *
     * @param ts the catalog tables to register
     * @return this builder
     */
    public Worker registerCatalogTables(Iterable<? extends CatalogTable> ts) {
        for (CatalogTable t : ts) catalogTables.add(t);
        return this;
    }

    /**
     * Catalog name surfaced through {@code catalog_catalogs()}.
     *
     * @return the catalog name (default {@code "vgi"})
     */
    public String catalogName() { return catalogName; }

    /**
     * Catalog comment surfaced through {@code catalog_catalogs()}.
     *
     * @return the catalog comment (default empty, never {@code null})
     */
    public String catalogComment() { return catalogComment; }

    /**
     * Catalog tags surfaced through {@code catalog_catalogs()}.
     *
     * @return the catalog tag key/value pairs, in insertion order
     */
    public Map<String, String> catalogTags() { return catalogTags; }

    /**
     * Schema DuckDB selects by default after ATTACH.
     *
     * @return the default schema name (default {@code "main"})
     */
    public String defaultSchema() { return defaultSchema; }

    /**
     * Session settings advertised at attach time.
     *
     * @return the registered setting specs, in registration order
     */
    public List<SettingSpec> settingSpecs() { return settings; }

    /**
     * The registered scalar functions (for landing-surface introspection).
     *
     * @return the scalar functions, in registration order
     */
    public List<ScalarFunction> scalars() { return scalars; }

    /**
     * The registered table functions (for landing-surface introspection).
     *
     * @return the table functions, in registration order
     */
    public List<TableFunction> tables() { return tables; }

    /**
     * The registered table-in-out functions (for landing-surface introspection).
     *
     * @return the table-in-out functions, in registration order
     */
    public List<TableInOutFunction> tableInOuts() { return tableInOuts; }

    /**
     * The registered aggregate functions (for landing-surface introspection).
     *
     * @return the aggregate functions, in registration order
     */
    public List<AggregateFunction<?>> aggregates() { return aggregates; }

    /**
     * The transport a server is being built for. Only {@link #HTTP} changes what is hosted
     * (opaque-data sealing and {@code vgi_rpc.Identity.v1}); the rest are named so the decision
     * lives in {@link #buildServer} rather than at each call site.
     */
    enum Transport { PIPE, UNIX, TCP, IROH, HTTP }

    /**
     * The one place this worker's {@link RpcServer} is built, for every transport.
     *
     * <p>What a server hosts, in reflection order:
     * <ol>
     *   <li>{@code vgi.v2};</li>
     *   <li>the {@link #hostedProtocols} hook's result, in the order returned -- on
     *       <strong>every</strong> transport;</li>
     *   <li>{@code vgi.attach_tickets.v1} -- HTTP only, and only when a signing key is configured
     *       ({@link #signingKey(byte[])}, {@link #opaqueDataKey(byte[])} or {@code VGI_SIGNING_KEY})
     *       and the worker can issue grants (grant keys, or {@link #mintGrant});</li>
     *   <li>{@code vgi_rpc.Reflection.v1} -- always, on every transport;</li>
     *   <li>{@code vgi_rpc.Identity.v1} -- HTTP only, and only when {@link #resolveToken},
     *       {@link #mintGrant} or grant keys are set.</li>
     * </ol>
     *
     * <p>Opaque-data sealing (AEAD of attach / transaction opaque data) is HTTP-only: on stdio /
     * AF_UNIX OS process ownership already enforces caller identity. When the caller configured
     * {@link #opaqueDataKey(byte[])}, that key is passed down so multi-replica deployments share
     * a key.
     *
     * @param transport the transport this server will serve
     * @return the configured server, not yet bound to any transport
     * @throws IllegalArgumentException if the {@link #hostedProtocols} hook returned an invalid
     *     protocol, or identity is enabled on HTTP without an introspector allowlist
     */
    RpcServer buildServer(Transport transport) {
        boolean http = transport == Transport.HTTP;
        byte[] signingKey = http ? configuredSigningKey() : null;
        RpcServer server = new RpcServer(VgiService.class,
                new VgiServiceImpl(this, scalars, tables, tableInOuts, aggregates,
                        http, signingKey));
        server.setProtocolVersion(advertisedProtocolVersion());
        hostExtraProtocols(server);
        // Identity -- sealed grants included -- is HTTP-only: its allowlist and freshness rules
        // read a caller principal, which stdin/stdout, AF_UNIX and TCP do not have. The port reads
        // VGI_RPC_GRANT_KEYS on its own, so the other transports turn it off explicitly.
        farm.query.vgirpc.identity.GrantKeys keys = http ? effectiveGrantKeys() : null;
        server.setGrantKeys(keys);
        // Attach tickets need both halves of an unattended session: a key that survives restarts
        // (configured, never the per-process one) and the ability to issue the grant that
        // presents the ticket. Absent otherwise, not hosted-and-refusing.
        if (http && signingKey != null && (keys != null || mintGrantHook != null)) {
            server.addProtocol(AttachTicketsService.class,
                    new farm.query.vgi.internal.AttachTicketsImpl(this, signingKey, ticketMaxTtl(keys)));
        }
        if (http) {
            farm.query.vgirpc.identity.IdentityImpl identity = buildIdentity(keys);
            if (identity != null) server.setIdentity(identity);
        }
        return server;
    }

    /** Call the {@link #hostedProtocols} hook once and register its result, naming the hook in
     *  every refusal -- the port's own messages name a protocol class, not the method to fix. */
    private void hostExtraProtocols(RpcServer server) {
        if (hostedProtocolsHook == null) return;
        String owner = "Worker.hostedProtocols hook";
        java.util.Collection<HostedProtocol> pairs = hostedProtocolsHook.get();
        if (pairs == null) return;
        int index = 0;
        for (HostedProtocol pair : pairs) {
            if (pair == null) {
                throw new IllegalArgumentException(owner + " entry " + index + " is null.");
            }
            String name;
            try {
                name = farm.query.vgirpc.ServiceIntrospector.protocolName(pair.protocol());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(owner + " entry " + index + " ("
                        + pair.protocol().getName() + "): " + e.getMessage(), e);
            }
            if (name.startsWith(farm.query.vgirpc.ProtocolNames.RESERVED_PREFIX)) {
                throw new IllegalArgumentException(owner + " entry " + index + " ("
                        + pair.protocol().getName() + ") is named '" + name + "', which claims the "
                        + "reserved 'vgi_rpc.' prefix. Framework protocols are not supplied through "
                        + "this hook: reflection is hosted automatically, and vgi_rpc.Identity.v1 is "
                        + "enabled by Worker.resolveToken() and/or Worker.mintGrant().");
            }
            if (name.equals(server.protocolName())) {
                throw new IllegalArgumentException(owner + " entry " + index + " ("
                        + pair.protocol().getName() + ") is named '" + name + "', the worker's own "
                        + "protocol. Give it a distinct @ProtocolName.");
            }
            try {
                server.addProtocol(pair.protocol(), pair.implementation());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(owner + " entry " + index + " ("
                        + pair.protocol().getName() + "): " + e.getMessage(), e);
            }
            index++;
        }
    }

    /**
     * Build {@code vgi_rpc.Identity.v1}, or {@code null} when neither hook is set -- so the
     * protocol is <em>absent</em>, not hosted-and-refusing.
     *
     * @throws IllegalArgumentException when {@link #resolveToken} is set but no introspector
     *     allowlist is configured: introspection is a distinct capability from authentication,
     *     and refusing to start beats an open oracle
     */
    private farm.query.vgirpc.identity.IdentityImpl buildIdentity(farm.query.vgirpc.identity.GrantKeys keys) {
        if (resolveTokenHook == null && mintGrantHook == null && keys == null) return null;
        // With keys and no minter of the worker's own, the port installs the sealed minter; with
        // a resolver or keys, HttpServer appends the grant / resolveToken bearer authenticators.
        farm.query.vgirpc.identity.IdentityImpl.Builder b = farm.query.vgirpc.identity.IdentityImpl.builder()
                .resolveToken(resolveTokenHook)
                .mintGrant(mintGrantHook)
                .grantKeys(keys);
        if (maxAuthAge != null) b.maxAuthAge(maxAuthAge);
        if (resolveTokenHook != null) {
            List<String> principals = introspectPrincipals != null
                    ? introspectPrincipals : principalsFromEnvironment();
            if (principals.stream().noneMatch(p -> p != null && !p.isBlank())) {
                throw new IllegalArgumentException("Worker.resolveToken is set but no introspector "
                        + "allowlist is configured. Set Worker.introspectPrincipals(...) or "
                        + "VGI_INTROSPECT_PRINCIPALS (comma-separated) to the principal(s) -- "
                        + "typically your reverse proxy -- allowed to call introspect_token. "
                        + "Introspection is a distinct capability from authentication: allowing "
                        + "any authenticated caller lets any user resolve any other user's "
                        + "credential to its owner.");
            }
            b.introspectPrincipals(principals);
        }
        return b.build();
    }

    /**
     * The signing key configured for this worker, normalized to 32 bytes, or {@code null} when none
     * is: {@link #signingKey(byte[])} / {@link #opaqueDataKey(byte[])} when set, else
     * {@code VGI_SIGNING_KEY} (the UTF-8 of its value).
     */
    private byte[] configuredSigningKey() {
        if (opaqueDataKey != null) return opaqueDataKey.clone();
        String env = System.getenv(SIGNING_KEY_ENV);
        if (env == null || env.isEmpty()) return null;
        return AttachTickets.normalizeKey(env.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * The attach-ticket lifetime ceiling: the grant keys' maximum, else
     * {@code VGI_RPC_GRANT_MAX_TTL_SECONDS} when set, else none.
     *
     * @throws IllegalArgumentException when the environment value is not a positive integer
     */
    private static Long ticketMaxTtl(farm.query.vgirpc.identity.GrantKeys keys) {
        if (keys != null) return keys.maxTtlSeconds();
        String raw = System.getenv(farm.query.vgirpc.identity.GrantKeys.MAX_TTL_ENV);
        if (raw == null || raw.isBlank()) return null;
        long value;
        try {
            value = Long.parseLong(raw.strip());
        } catch (NumberFormatException e) {
            value = 0;
        }
        if (value <= 0) {
            throw new IllegalArgumentException(farm.query.vgirpc.identity.GrantKeys.MAX_TTL_ENV + "='" + raw
                    + "' must be a positive integer");
        }
        return value;
    }

    /** {@link #grantKeys} when set in code or by {@code --grant-key}, else the environment. */
    private farm.query.vgirpc.identity.GrantKeys effectiveGrantKeys() {
        return grantKeys != null ? grantKeys : farm.query.vgirpc.identity.GrantKeys.fromEnv();
    }

    private static List<String> principalsFromEnvironment() {
        String raw = System.getenv("VGI_INTROSPECT_PRINCIPALS");
        if (raw == null || raw.isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String p : raw.split(",")) {
            if (!p.isBlank()) out.add(p.trim());
        }
        return out;
    }

    /**
     * The application protocol version this worker advertises and enforces.
     *
     * <p>{@link #VGI_PROTOCOL_VERSION} unless {@code VGI_PROTOCOL_VERSION_OVERRIDE}
     * names another. The override exists for ONE purpose: the cross-language
     * suite needs a worker that deliberately declares an incompatible version,
     * so it can assert that the dispatch-boundary gate fires and that the error
     * says which side to upgrade. Every other SDK's fixture worker has the same
     * hook (vgi-rust's {@code ci/wrappers/vgi-worker-bad-protocol} sets exactly
     * this variable).</p>
     *
     * <p>It is deliberately not a public API: mis-declaring the protocol version
     * of a real worker makes it unreachable, which is the whole point of the
     * test and a disaster anywhere else.</p>
     *
     * @return the version to advertise
     */
    private static String advertisedProtocolVersion() {
        String override = System.getenv("VGI_PROTOCOL_VERSION_OVERRIDE");
        return override == null || override.isBlank() ? VGI_PROTOCOL_VERSION : override.trim();
    }

    /**
     * Build the {@link RpcServer} for this worker so a caller can drive it over
     * a transport it owns, instead of one of the blocking {@code run*} entry
     * points. Intended for embedding and for in-process tests that pair the
     * server with an {@code RpcConnection} client over a pipe.
     *
     * <p>Opaque-data sealing is disabled, matching {@link #runStdio()} and
     * {@link #runUnixSocket}: a caller-supplied transport carries no HTTP auth
     * principal to bind tokens to. Do not expose the returned server on an
     * untrusted network — use {@link #runHttp(String, int)} for that.
     *
     * @return a configured server, not yet bound to any transport
     */
    public RpcServer rpcServer() {
        return buildServer(Transport.PIPE);
    }

    /** Block on stdin/stdout serving requests until the transport closes. */
    public void runStdio() {
        try (StdioTransport t = new StdioTransport()) {
            buildServer(Transport.PIPE).serve(t);
        }
    }

    /**
     * Block accepting AF_UNIX connections on {@code socketPath}, dispatching
     * each client on a virtual thread, implementing the VGI launcher protocol:
     * the worker prints {@code UNIX:<path>\n} to stdout once the listener is
     * bound, then serves until the process is killed or the idle watchdog fires.
     *
     * <p>{@code idleTimeoutMs <= 0} means "no timeout" — the worker runs
     * until the launcher SIGTERMs it.
     *
     * @param socketPath    filesystem path the AF_UNIX listener binds to
     * @param idleTimeoutMs idle watchdog in milliseconds; {@code <= 0} disables it
     * @throws IOException if the socket cannot be bound or served
     */
    public void runUnixSocket(Path socketPath, long idleTimeoutMs) throws IOException {
        // idleTimeoutMs <= 0 → never time out (legacy behaviour). The
        // launcher passes --idle-timeout 300 by default; the watchdog inside
        // UnixSocketTransport.serveForever closes the listener when active
        // connections stay at zero past that boundary, letting the JVM exit.
        UnixSocketTransport.serveForever(socketPath, buildServer(Transport.UNIX), idleTimeoutMs);
    }

    /**
     * Block accepting TCP connections on {@code host}:{@code port}, dispatching
     * each client on a virtual thread, implementing the VGI launcher protocol:
     * the worker prints {@code TCP:<host>:<port>\n} to stdout once the listener
     * is bound (the actual port, so {@code port == 0} ephemeral binds are
     * discoverable), then serves until killed or the idle watchdog fires.
     *
     * <p>Raw TCP framing carries <strong>no authentication or encryption</strong>
     * — bind it to loopback / a trusted network only; use {@link #runHttp} for
     * untrusted networks.
     *
     * @param host          bind host ({@code "127.0.0.1"} for loopback)
     * @param port          bind port; {@code 0} selects a free port
     * @param idleTimeoutMs idle watchdog in milliseconds; {@code <= 0} disables it
     * @throws IOException if the socket cannot be bound or served
     */
    public void runTcp(String host, int port, long idleTimeoutMs) throws IOException {
        TcpSocketTransport.serveForever(host, port, buildServer(Transport.TCP), idleTimeoutMs,
                (boundHost, boundPort) -> {
                    System.out.println("TCP:" + boundHost + ":" + boundPort);
                    System.out.flush();
                });
    }

    /**
     * Serve the identity-preserving raw upstream consumed by
     * {@code vgi-iroh-bridge}. The upstream is loopback-only and requires the
     * bridge's EndpointId-bearing PROXY-v2 preamble on every connection.
     *
     * @param host loopback bind host
     * @param port bind port; {@code 0} selects a free port
     * @param idleTimeoutMs idle watchdog in milliseconds; {@code <= 0} disables it
     * @param bridge trusted bridge identity configuration
     * @throws IOException if the socket cannot be bound or served
     */
    public void runIrohTcpUpstream(
            String host, int port, long idleTimeoutMs, IrohBridgeOptions bridge) throws IOException {
        requireLoopback(host, "Iroh raw bridge upstream");
        if (bridge == null) throw new IllegalArgumentException("Iroh bridge options are required");
        TcpSocketTransport.serveForever(
                host,
                port,
                buildServer(Transport.IROH),
                idleTimeoutMs,
                (boundHost, boundPort) -> {
                    System.out.println("TCP:" + boundHost + ":" + boundPort);
                    System.out.flush();
                },
                bridge.tcpServerOptions());
    }

    /**
     * Parsed {@code [HOST:]PORT} TCP bind spec. Host defaults to loopback.
     *
     * @param host bind host
     * @param port bind port ({@code 0} = ephemeral)
     */
    public record TcpAddr(String host, int port) {}

    /**
     * Parse a {@code [HOST:]PORT} TCP bind spec as accepted by {@code --tcp}.
     * A bare {@code PORT} binds {@code 127.0.0.1}; an empty host (leading
     * {@code ":"}) also defaults to loopback.
     *
     * @param spec the {@code [HOST:]PORT} string
     * @return the parsed host/port
     */
    public static TcpAddr parseTcpAddr(String spec) {
        int idx = spec.lastIndexOf(':');
        if (idx >= 0) {
            String h = spec.substring(0, idx);
            return new TcpAddr(h.isEmpty() ? "127.0.0.1" : h,
                    Integer.parseInt(spec.substring(idx + 1)));
        }
        return new TcpAddr("127.0.0.1", Integer.parseInt(spec));
    }

    /**
     * Run as an HTTP server bound to {@code host}/{@code port}, blocking until shutdown.
     *
     * @param host bind host
     * @param port bind port ({@code 0} for an ephemeral port)
     * @throws Exception if the server fails to start or serve
     */
    public void runHttp(String host, int port) throws Exception {
        runHttp(HttpServer.Config.builder().host(host).port(port).build());
    }

    /**
     * Run the ordinary VGI HTTP server behind {@code vgi-iroh-bridge}, retaining
     * HTTP limits, continuations, externalized batches, and the authenticated
     * client EndpointId.
     *
     * @param host loopback bind host
     * @param port bind port; {@code 0} selects a free port
     * @param bridge trusted bridge identity configuration
     * @throws Exception if the server fails to start or serve
     */
    public void runHttp(String host, int port, IrohBridgeOptions bridge) throws Exception {
        requireLoopback(host, "Iroh HTTP bridge upstream");
        if (bridge == null) throw new IllegalArgumentException("Iroh bridge options are required");
        runHttp(bridge.apply(HttpServer.Config.builder().host(host).port(port)).build());
    }

    /**
     * Canonical CLI dispatcher used by worker {@code main} methods. Parses
     * the transport flags every VGI worker accepts and runs the matching transport:
     * <ul>
     *   <li>{@code --unix <path>}: AF_UNIX socket (launcher protocol)
     *   <li>{@code --tcp [<host>:]<port>}: TCP socket (launcher protocol)
     *   <li>{@code --http} with optional {@code --host}, {@code --port}: HTTP
     *   <li>{@code --iroh-raw-upstream [<host>:]<port>}: trusted raw bridge upstream
     *   <li>{@code --iroh-issuer}, repeated {@code --iroh-trusted-proxy}, and
     *       {@code --iroh-observe}: Iroh bridge trust and authentication mode
     *   <li>{@code --idle-timeout <seconds>}: passed to {@code runUnixSocket} / {@code runTcp}
     *   <li>{@code --grant-key <base64>}: sealed-grant key, repeatable, first mints (HTTP; see
     *       {@link #grantKeys})
     *   <li>(default): stdio
     * </ul>
     * Also honours {@code VGI_WORKER_STDERR}: redirects {@link System#err} to
     * the named file (in append mode) before any other work, so
     * launcher-mode crashes — where the launcher dup2's {@code /dev/null}
     * over fd 2 — remain inspectable.
     *
     * <p>Unknown args exit with status 2; transport-run failures with 1.
     *
     * @param args  argv as received by {@code main}
     * @param httpCustomizer  applied to the {@code HttpServer.Config.Builder}
     *        seeded with {@code host} and {@code port}; use {@code b -> b}
     *        for defaults, or to layer in JWT / TLS / byte-limit config
     */
    public void runFromArgs(String[] args,
                             java.util.function.UnaryOperator<HttpServer.Config.Builder> httpCustomizer) {
        String stderrPath = System.getenv("VGI_WORKER_STDERR");
        if (stderrPath != null && !stderrPath.isEmpty()) {
            try {
                System.setErr(new java.io.PrintStream(
                        new java.io.FileOutputStream(stderrPath, true), true));
            } catch (Exception ignore) {}
        }
        boolean http = false;
        String host = "127.0.0.1";
        int port = 0;
        String unixSocket = null;
        String tcpAddr = null;
        String irohRawUpstream = null;
        String irohIssuer = null;
        List<String> irohTrustedProxies = new ArrayList<>();
        List<String> grantKeyArgs = new ArrayList<>();
        boolean irohObserve = false;
        long idleTimeoutMs = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--http" -> http = true;
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--unix" -> unixSocket = args[++i];
                case "--tcp" -> tcpAddr = args[++i];
                case "--iroh-raw-upstream" -> irohRawUpstream = args[++i];
                case "--iroh-issuer" -> irohIssuer = args[++i];
                case "--iroh-trusted-proxy" -> irohTrustedProxies.add(args[++i]);
                case "--iroh-observe" -> irohObserve = true;
                case "--grant-key" -> grantKeyArgs.add(args[++i]);
                case "--idle-timeout" -> idleTimeoutMs =
                        (long) (Double.parseDouble(args[++i]) * 1000.0);
                case "--describe", "--no-describe", "--threaded", "--quiet", "-q", "--debug" -> { }
                case "--log-level" -> i++;
                default -> { System.err.println("unknown arg: " + args[i]); System.exit(2); }
            }
        }
        if (!grantKeyArgs.isEmpty()) {
            // --grant-key KEY, repeatable, first mints; audience and lifetime still come from
            // VGI_RPC_GRANT_AUDIENCE / VGI_RPC_GRANT_MAX_TTL_SECONDS. A malformed key stops here.
            String ttl = System.getenv(farm.query.vgirpc.identity.GrantKeys.MAX_TTL_ENV);
            grantKeys(farm.query.vgirpc.identity.GrantKeys.parse(grantKeyArgs,
                    System.getenv().getOrDefault(farm.query.vgirpc.identity.GrantKeys.AUDIENCE_ENV, ""),
                    ttl == null || ttl.isBlank()
                            ? farm.query.vgirpc.identity.GrantKeys.DEFAULT_MAX_TTL_SECONDS
                            : Long.parseLong(ttl.strip())));
        }
        int selectedTransports = (http ? 1 : 0)
                + (unixSocket != null ? 1 : 0)
                + (tcpAddr != null ? 1 : 0)
                + (irohRawUpstream != null ? 1 : 0);
        if (selectedTransports > 1) {
            throw new IllegalArgumentException(
                    "--http, --unix, --tcp, and --iroh-raw-upstream are mutually exclusive");
        }
        if ((irohIssuer != null || !irohTrustedProxies.isEmpty() || irohObserve)
                && irohRawUpstream == null && !http) {
            throw new IllegalArgumentException(
                    "Iroh bridge options require --http or --iroh-raw-upstream");
        }
        if ((irohRawUpstream != null || !irohTrustedProxies.isEmpty() || irohObserve)
                && irohIssuer == null) {
            throw new IllegalArgumentException("Iroh bridge options require --iroh-issuer");
        }
        try {
            if (unixSocket != null) {
                runUnixSocket(Path.of(unixSocket), idleTimeoutMs);
            } else if (tcpAddr != null) {
                TcpAddr a = parseTcpAddr(tcpAddr);
                runTcp(a.host(), a.port(), idleTimeoutMs);
            } else if (irohRawUpstream != null) {
                if (irohIssuer == null) {
                    throw new IllegalArgumentException(
                            "--iroh-raw-upstream requires --iroh-issuer");
                }
                TcpAddr a = parseTcpAddr(irohRawUpstream);
                runIrohTcpUpstream(a.host(), a.port(), idleTimeoutMs,
                        IrohBridgeOptions.fromArgs(
                                irohIssuer, irohTrustedProxies, !irohObserve));
            } else if (http) {
                HttpServer.Config.Builder b = HttpServer.Config.builder().host(host).port(port);
                if (httpCustomizer != null) b = httpCustomizer.apply(b);
                if (irohIssuer != null) {
                    requireLoopback(host, "Iroh HTTP bridge upstream");
                    b = IrohBridgeOptions.fromArgs(
                            irohIssuer, irohTrustedProxies, !irohObserve).apply(b);
                }
                runHttp(b.build());
            } else {
                runStdio();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Convenience overload — equivalent to
     *  {@code runFromArgs(args, b -> b)}.
     *
     * @param args argv as received by {@code main}
     */
    public void runFromArgs(String[] args) {
        runFromArgs(args, b -> b);
    }

    /** HTTP variant that accepts a fully-built config (prefix, authenticator,
     *  TLS, byte limits, …). Used by workers that wire OAuth/JWT or other
     *  production knobs from environment variables.
     *
     *  <p>On SIGTERM the shutdown hook fires, calling {@link HttpServer#stop()}.
     *  Jetty awaits in-flight requests up to its configured stop timeout
     *  (15 s by default; see {@code HttpServer}'s {@code setStopTimeout}) and
     *  then forcibly closes any stragglers.
     *
     * @param config fully-built HTTP server configuration
     * @throws Exception if the server fails to start or serve
     */
    public void runHttp(HttpServer.Config config) throws Exception {
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Worker.class);
        // Attach the standardized landing surface (the shared page plus the
        // browser client build it reads the catalog with) unless the caller
        // already supplied an identity.
        HttpServer.Config effective = config.landingInfo() != null
                ? config
                : config.withLandingInfo(farm.query.vgi.http.WorkerLandingInfo.of(this));
        HttpServer http = new HttpServer(buildServer(Transport.HTTP), effective);
        http.start();
        System.out.println("PORT:" + http.port());
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("SIGTERM received — stopping HTTP server (graceful, up to 15s)");
            long t0 = System.currentTimeMillis();
            try {
                http.stop();
                log.info("HTTP server stopped after {} ms", System.currentTimeMillis() - t0);
            } catch (Exception e) {
                log.warn("HTTP server stop failed after {} ms: {}",
                        System.currentTimeMillis() - t0, e.toString());
            }
        }, "vgi-http-shutdown"));
        http.join();
    }

    private static void requireLoopback(String host, String label) {
        if (!("127.0.0.1".equals(host) || "::1".equals(host) || "localhost".equals(host))) {
            throw new IllegalArgumentException(label + " must bind loopback, got " + host);
        }
    }
}
