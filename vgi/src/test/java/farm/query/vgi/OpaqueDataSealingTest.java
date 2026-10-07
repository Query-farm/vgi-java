// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.catalog.InMemoryCatalog;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.OpaqueDataSealer;
import farm.query.vgi.internal.VgiServiceImpl;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.http.Authenticator;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpServer;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sealing of {@code attach_opaque_data} and {@code transaction_opaque_data}
 * ({@code docs/protocol/vgi-opaque-data-sealing.md}), over HTTP through the RPC layer: tampering,
 * replay as another caller, a transaction replayed under another attach, and every plaintext
 * shape this SDK ever produced are all rejected with one identical error per field; there is no
 * unsealed fallback; a secret attach option never reaches an opaque value in plaintext on any
 * transport; and no opaque value is printed.
 */
@Timeout(120)
final class OpaqueDataSealingTest {

    private static final byte[] KEY = new byte[32];
    private static final String MAIN = "sealed_main";
    private static final String EXTRA = "sealed_extra";
    private static final String HOSTED = "sealed_mem";
    private static final String SECRET = "sk-SEALING-0123456789abcdef";

    static {
        for (int i = 0; i < KEY.length; i++) KEY[i] = (byte) (0x55 + i);
    }

    /** {@code X-Test-Principal} logs in (domain {@code login}); no header is anonymous. */
    private static final Authenticator LOGIN = req -> {
        String p = req.getHeader("X-Test-Principal");
        return p == null ? AuthContext.ANONYMOUS : new AuthContext("login", true, p, Map.of());
    };

    private static HttpServer http;
    private static String url;
    private static final ByteArrayOutputStream PRINTED = new ByteArrayOutputStream();
    private static PrintStream savedErr;
    private static PrintStream savedOut;

    private static Worker worker() {
        return Worker.builder().catalogName(MAIN)
                .attachOptions(
                        AttachOptionSpec.of("region", "plain", new ArrowType.Utf8(), "us-east-1"),
                        AttachOptionSpec.requiredSecret("api_key", "a credential", new ArrowType.Utf8()))
                .registerExtraCatalog(new Worker.ExtraCatalog(EXTRA, null, null, "an auxiliary catalog"))
                .registerCatalog(new InMemoryCatalog(HOSTED));
    }

    @BeforeAll
    static void start() throws Exception {
        // Everything the worker prints while serving, so a raw value reaching a log is caught.
        savedErr = System.err;
        savedOut = System.out;
        System.setErr(new PrintStream(new TeeStream(savedErr, PRINTED), true, StandardCharsets.UTF_8));
        System.setOut(new PrintStream(new TeeStream(savedOut, PRINTED), true, StandardCharsets.UTF_8));
        RpcServer rpc = worker().opaqueDataKey(KEY).buildServer(Worker.Transport.HTTP);
        http = new HttpServer(rpc, HttpServer.Config.builder().host("127.0.0.1").port(0).authenticator(LOGIN).build());
        http.start();
        url = "http://127.0.0.1:" + http.port();
    }

    @AfterAll
    static void stop() throws Exception {
        if (http != null) http.stop();
        System.setErr(savedErr);
        System.setOut(savedOut);
    }

    private static HttpRpcConnection as(String principal) {
        HttpRpcConnection.Builder b = HttpRpcConnection.builder(url);
        if (principal != null) b.header("X-Test-Principal", principal);
        return b.build();
    }

    private static byte[] options() {
        return options(Map.of("api_key", SECRET, "region", "eu-west-2"));
    }

    private static byte[] options(Map<String, String> values) {
        Map<String, String> ordered = new LinkedHashMap<>(values);
        List<Field> fields = new ArrayList<>();
        for (String n : ordered.keySet()) fields.add(new Field(n, FieldType.nullable(new ArrowType.Utf8()), null));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            int i = 0;
            for (String v : ordered.values()) ((VarCharVector) root.getVector(i++)).setSafe(0, v.getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static byte[] attach(VgiService vgi, String catalog) {
        byte[] opts = MAIN.equals(catalog) ? options() : null;
        return vgi.catalog_attach(CatalogAttachRequest.of(catalog, opts, "", ""), null).attach_opaque_data();
    }

    private static byte[] flip(byte[] value, int at) {
        byte[] out = value.clone();
        out[at] ^= 0x01;
        return out;
    }

    /** The parts of an error a probing caller can see. */
    private record Seen(String type, String message, String kind, String code, String details) {
        static Seen of(RpcError e) {
            return new Seen(e.errorType(), e.errorMessage(), e.errorKind(), e.errorCode(),
                    String.valueOf(e.errorDetails()));
        }
    }

    private static Seen refused(Runnable call) {
        return Seen.of(assertThrows(RpcError.class, call::run));
    }

    private static AuthContext login(String principal) {
        return new AuthContext("login", true, principal, Map.of());
    }

    @Test
    void everyFailureModeIsTheSameRejection() {
        try (HttpRpcConnection alice = as("alice"); HttpRpcConnection bob = as("bob");
             HttpRpcConnection anonymous = as(null)) {
            VgiService a = alice.proxy(VgiService.class);
            VgiService b = bob.proxy(VgiService.class);
            VgiService anon = anonymous.proxy(VgiService.class);

            // The owner's own use works first, so a worker that rejects everything does not pass.
            byte[] att = attach(a, MAIN);
            byte[] att2 = attach(a, MAIN);
            byte[] txn = a.catalog_transaction_begin(att, null).transaction_opaque_data();
            a.catalog_version(att, null, null);
            a.catalog_version(att, txn, null);
            assertFalse(a.catalog_schemas(att, null, null).items().isEmpty());

            List<Seen> attachErrors = new ArrayList<>();
            for (int at : new int[] {0, att.length / 2, att.length - 1}) {
                byte[] tampered = flip(att, at);
                attachErrors.add(refused(() -> a.catalog_version(tampered, null, null)));
            }
            attachErrors.add(refused(() -> b.catalog_version(att, null, null)));
            attachErrors.add(refused(() -> anon.catalog_version(att, null, null)));
            for (Supplier<byte[]> forged : forgedShapes(att)) {
                byte[] value = forged.get();
                attachErrors.add(refused(() -> a.catalog_version(value, null, null)));
            }
            Seen attachRejection = attachErrors.get(0);
            assertEquals("attach_opaque_data not recognized", attachRejection.message(), attachRejection.toString());
            assertEquals("opaque_data_not_recognized", attachRejection.kind());
            assertEquals("INVALID_ARGUMENT", attachRejection.code());
            assertEquals("[]", attachRejection.details(), "no details");
            for (Seen s : attachErrors) assertEquals(attachRejection, s, "one rejection for every attach failure");

            List<Seen> txnErrors = new ArrayList<>();
            for (int at : new int[] {0, txn.length / 2, txn.length - 1}) {
                byte[] tampered = flip(txn, at);
                txnErrors.add(refused(() -> a.catalog_version(att, tampered, null)));
            }
            // Same principal, same catalog, another attach: still not this transaction's parent.
            txnErrors.add(refused(() -> a.catalog_version(att2, txn, null)));
            txnErrors.add(refused(() -> a.catalog_transaction_commit(att2, txn, null)));
            byte[] bobAttach = attach(b, MAIN);
            txnErrors.add(refused(() -> b.catalog_version(bobAttach, txn, null)));
            txnErrors.add(refused(() -> a.catalog_version(att, new byte[] {1, 2, 3}, null)));
            Seen txnRejection = txnErrors.get(0);
            assertEquals("transaction_opaque_data not recognized", txnRejection.message());
            for (Seen s : txnErrors) assertEquals(txnRejection, s, "one rejection for every transaction failure");
            assertEquals(attachRejection.message().replace("attach_opaque_data", "transaction_opaque_data"),
                    txnRejection.message(), "the two fields differ only in the field name");
            assertEquals(new Seen(attachRejection.type(), txnRejection.message(), attachRejection.kind(),
                    attachRejection.code(), attachRejection.details()), txnRejection);
        }
    }

    /** Values of every shape this SDK ever produced unsealed, and a random envelope-shaped one. */
    private static List<Supplier<byte[]>> forgedShapes(byte[] sealed) {
        SecureRandom rng = new SecureRandom();
        byte[] uuid = new byte[16];
        rng.nextBytes(uuid);
        byte[] ipc = options();
        byte[] withOptions = new byte[16 + 1 + ipc.length];
        System.arraycopy(uuid, 0, withOptions, 0, 16);
        System.arraycopy(ipc, 0, withOptions, 17, ipc.length);
        byte[] envelope = new byte[sealed.length];
        rng.nextBytes(envelope);
        envelope[0] = sealed[0];
        return List.of(
                () -> uuid,
                () -> withOptions,
                () -> ("writable:" + HexFormat.of().formatHex(uuid)).getBytes(StandardCharsets.UTF_8),
                () -> ipc,
                () -> "{\"catalog\":\"sealed_main\"}".getBytes(StandardCharsets.UTF_8),
                () -> envelope,
                () -> new byte[0]);
    }

    /**
     * The plaintext id the worker would accept on an OS-owned transport is refused on HTTP, on
     * every catalog kind and every route that resolves a catalog -- including the calls that
     * used to look a value up unsealed when they carried no call context.
     */
    @Test
    void thePlaintextIdIsRefusedOnEveryRoute() {
        OpaqueDataSealer opener = new OpaqueDataSealer(KEY);
        try (HttpRpcConnection alice = as("alice")) {
            VgiService a = alice.proxy(VgiService.class);
            for (String catalog : List.of(MAIN, EXTRA, HOSTED)) {
                byte[] sealed = attach(a, catalog);
                byte[] plain = opener.unsealAttach(sealed, login("alice"));
                a.catalog_schemas(sealed, null, null);
                for (Runnable probe : List.<Runnable>of(
                        () -> a.catalog_version(plain, null, null),
                        () -> a.catalog_schemas(plain, null, null),
                        () -> a.catalog_schema_get(plain, List.of("main"), null, null),
                        () -> a.catalog_schema_contents_views(plain, List.of("main"), null, null),
                        () -> a.catalog_copy_from_formats(plain, null, null),
                        () -> a.catalog_transaction_begin(plain, null))) {
                    Seen s = refused(probe);
                    assertTrue(s.message().contains("attach_opaque_data not recognized"), catalog + ": " + s);
                }
            }
        }
    }

    @Test
    void aSecretOptionIsNeverInPlaintextOnAnyTransport() {
        // OS-owned transport: no sealing, so the value is the plain attach id.
        Worker w = worker();
        VgiServiceImpl unsealed = new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
        byte[] plain = unsealed.catalog_attach(CatalogAttachRequest.of(MAIN, options(), "", ""), null)
                .attach_opaque_data();
        assertSecretAbsent(plain);
        assertTrue(new String(plain, StandardCharsets.ISO_8859_1).contains("eu-west-2"),
                "the non-secret option is still carried");
        assertSecretAbsent(unsealed.catalog_transaction_begin(plain, null).transaction_opaque_data());

        // HTTP: sealed.
        try (HttpRpcConnection alice = as("alice")) {
            VgiService a = alice.proxy(VgiService.class);
            byte[] sealed = attach(a, MAIN);
            assertSecretAbsent(sealed);
            assertSecretAbsent(a.catalog_transaction_begin(sealed, null).transaction_opaque_data());
            assertSecretAbsent(new OpaqueDataSealer(KEY).unsealAttach(sealed, login("alice")));
        }
    }

    private static void assertSecretAbsent(byte[] value) {
        byte[] secret = SECRET.getBytes(StandardCharsets.UTF_8);
        String hex = HexFormat.of().formatHex(value);
        assertFalse(contains(value, secret), "the secret as bytes");
        assertFalse(hex.contains(HexFormat.of().formatHex(secret)), "the secret as hex");
        assertFalse(Base64.getEncoder().encodeToString(value).contains(Base64.getEncoder().encodeToString(secret)),
                "the secret as base64");
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) if (haystack[i + j] != needle[j]) continue outer;
            return true;
        }
        return false;
    }

    @Test
    void noOpaqueValueIsPrinted() {
        List<byte[]> values = new ArrayList<>();
        try (HttpRpcConnection alice = as("alice"); HttpRpcConnection bob = as("bob")) {
            VgiService a = alice.proxy(VgiService.class);
            byte[] att = attach(a, MAIN);
            byte[] txn = a.catalog_transaction_begin(att, null).transaction_opaque_data();
            values.add(att);
            values.add(txn);
            a.catalog_version(att, txn, null);
            assertThrows(RpcError.class, () -> bob.proxy(VgiService.class).catalog_version(att, txn, null));
            assertThrows(RpcError.class, () -> a.catalog_version(flip(att, 3), null, null));
        }
        String printed = PRINTED.toString(StandardCharsets.UTF_8);
        assertFalse(printed.contains(SECRET), "the secret was printed");
        for (byte[] v : values) {
            String hex = HexFormat.of().formatHex(v);
            for (int i = 0; i + 24 <= hex.length(); i += 2) {
                assertFalse(printed.contains(hex.substring(i, i + 24)), "a 12-byte window of an opaque value was printed");
            }
            assertFalse(printed.contains(Base64.getEncoder().encodeToString(v)), "an opaque value was printed as base64");
        }
    }

    /** Writes to the real stream and a capture buffer. */
    private static final class TeeStream extends java.io.OutputStream {
        private final java.io.OutputStream a;
        private final java.io.OutputStream b;

        TeeStream(java.io.OutputStream a, java.io.OutputStream b) {
            this.a = a;
            this.b = b;
        }

        @Override public synchronized void write(int c) throws java.io.IOException {
            a.write(c);
            b.write(c);
        }

        @Override public synchronized void write(byte[] buf, int off, int len) throws java.io.IOException {
            a.write(buf, off, len);
            b.write(buf, off, len);
        }

        @Override public void flush() throws java.io.IOException {
            a.flush();
            b.flush();
        }
    }
}
