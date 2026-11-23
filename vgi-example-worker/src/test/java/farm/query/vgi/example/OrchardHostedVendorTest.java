// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import farm.query.vgi.VgiService;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.protocol.ArgumentsLocation;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.ClientCapabilities;
import farm.query.vgi.protocol.Nested;
import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgirpc.external.ExternalLocationConfig;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.UploadRequest;
import farm.query.vgirpc.http.UploadUrl;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Orchard hosted mode with the blob vendor (the Java port of vgi-csharp's
 * {@code OrchardHostedVendorTests}; vgi-python's hosted mode reads the same environment).
 *
 * <p>Runs the example worker as a real process with {@code VGI_ORCHARD_HOSTED=1} plus
 * {@code VGI_BLOB_VENDOR_URL} / {@code VGI_BLOB_VENDOR_TOKEN} against vgi-rpc-python's fake vendor,
 * and checks that the hosted worker advertises the capability headers (upload URLs v2,
 * {@code VGI-Arguments-Ref-Bytes}, externalization, the 32 MiB hosted request cap) on accepted
 * and 401 responses; that an {@code __upload_url__} grant carries {@code object_id} and a
 * {@code ref}; that the ref opens for the Pass principal it was granted to and is refused
 * ({@code invalid_arguments_ref}) for another; and that a plan over the threshold is externalized.
 *
 * <p>Set {@code VGI_RPC_FAKE_VENDOR_BIN} to the {@code vgi-rpc-fake-vendor} executable to run it
 * (CI does); without it the test is skipped.
 */
final class OrchardHostedVendorTest {

    private static final String ISSUER = "https://auth.orchard.test";
    private static final String AUDIENCE = "https://desk.orchard.test/d/dep_123";
    private static final String VENDOR_TOKEN = "test-token";
    private static final int SPLITS = 400;

    @Test
    @Timeout(240)
    void grantsBindToThePassPrincipalAndLargeResultsAreExternalized() throws Exception {
        String bin = System.getenv("VGI_RPC_FAKE_VENDOR_BIN");
        Assumptions.assumeTrue(bin != null && !bin.isBlank(),
                "Set VGI_RPC_FAKE_VENDOR_BIN to vgi-rpc-python's vgi-rpc-fake-vendor to run the hosted blob-vendor test.");

        ECKey ec = new ECKeyGenerator(Curve.P_256).keyID("desk-es256-1").generate();
        byte[] jwksBody = new JWKSet(ec.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);
        com.sun.net.httpserver.HttpServer jwks =
                com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jwks.createContext("/jwks", ex -> {
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, jwksBody.length);
            ex.getResponseBody().write(jwksBody);
            ex.getResponseBody().close();
        });
        jwks.start();
        Process vendor = null;
        Process worker = null;
        try {
            ProcessBuilder vb = new ProcessBuilder(bin, "--test-routes", "--token", VENDOR_TOKEN + "=tenant-a",
                    "--port", "0");
            vb.redirectError(ProcessBuilder.Redirect.DISCARD);
            vendor = vb.start();
            String vendorUrl = readPrefixed(vendor, "URL:", 120);
            assertTrue(vendorUrl != null, "the fake vendor did not print URL:");

            Path log = Files.createTempFile("orchard-hosted-vendor-worker", ".log");
            ProcessBuilder pb = new ProcessBuilder(command("--http", "--host", "127.0.0.1", "--port", "0"));
            Map<String, String> env = pb.environment();
            for (String k : List.of("VGI_SIGNING_KEY_MINTED", "VGI_BEARER_TOKENS", "VGI_JWT_ALGORITHMS",
                    "VGI_RPC_GRANT_KEYS", "VGI_OAUTH_RESOURCE", "VGI_OAUTH_AUTH_SERVERS", "VGI_PROXY_PROOF_MODE",
                    "VGI_INTROSPECT_PRINCIPALS", "VGI_TEST_BEARER_TOKEN", "VGI_FIXTURE_IDENTITY",
                    "VGI_WORKER_CATALOG_NAME", "VGI_WORKER_STDERR", "VGI_RPC_MAX_REQUEST_BYTES",
                    "VGI_TEST_DISABLE_ARGUMENTS_REF", "VGI_ARGUMENTS_REF_BYTES")) {
                env.remove(k);
            }
            env.put("VGI_ORCHARD_HOSTED", "1");
            env.put("VGI_JWT_ISSUER", ISSUER);
            env.put("VGI_JWT_AUDIENCE", AUDIENCE);
            env.put("VGI_JWT_JWKS_URI", "http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks");
            env.put("VGI_SIGNING_KEY", "orchard-hosted-test-signing-key-0001");
            env.put("VGI_BLOB_VENDOR_URL", vendorUrl);
            env.put("VGI_BLOB_VENDOR_TOKEN", VENDOR_TOKEN);
            env.put("VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096");
            pb.redirectError(log.toFile());
            worker = pb.start();
            String port = readPrefixed(worker, "PORT:", 120);
            if (port == null) fail("hosted worker did not start:\n" + Files.readString(log));
            String base = "http://127.0.0.1:" + port;
            String alice = pass(ec, "user_alice");
            String bob = pass(ec, "user_bob");

            // The capability headers, behind the Pass; a 401 tells the same truth.
            for (String token : new String[] {alice, null}) {
                HttpResponse<String> r = options(base + "/health", token);
                assertEquals(token == null ? 401 : 200, r.statusCode(), r.body());
                assertEquals("2", header(r, "VGI-Upload-URL-Support"));
                assertEquals("4194304", header(r, "VGI-Arguments-Ref-Bytes"));
                assertEquals("true", header(r, "VGI-Externalization-Enabled"));
                assertEquals(Long.toString(32L * 1024 * 1024), header(r, "VGI-Max-Request-Bytes"));
            }

            byte[] arguments = ArgumentsEncoder.builder().positional("p".repeat(5000)).positional((long) SPLITS).encode();
            String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(arguments));
            UploadUrl grant;
            try (HttpRpcConnection conn = connection(base, alice)) {
                grant = conn.requestUploadUrls(List.of(new UploadRequest(arguments.length, sha))).get(0);
            }
            assertFalse(grant.objectId().isEmpty(), "the grant has no object_id");
            assertTrue(grant.ref().length > 0, "the grant has no ref");
            try (HttpClient http = HttpClient.newHttpClient()) {
                HttpRequest.Builder put = HttpRequest.newBuilder(URI.create(grant.uploadUrl()))
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(arguments));
                grant.uploadHeaders().forEach(put::header);
                int status = http.send(put.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                assertTrue(status < 300, "vendor PUT answered " + status);
            }
            ArgumentsLocation location = new ArgumentsLocation(grant.ref(), arguments.length, sha);

            // The ref opens for the principal it was granted to, and for no other. Attach data is
            // sealed for its caller, so each principal attaches for itself.
            BindRequest aliceBind;
            try (HttpRpcConnection conn = connection(base, alice)) {
                VgiService vgi = conn.proxy(VgiService.class);
                aliceBind = bind(attach(vgi), location);
                BindRequest call = aliceBind;
                assertDoesNotThrow(() -> vgi.bind(call, null));
            }
            try (HttpRpcConnection conn = connection(base, bob)) {
                VgiService vgi = conn.proxy(VgiService.class);
                BindRequest call = bind(attach(vgi), location);
                Exception refused = assertThrows(Exception.class, () -> vgi.bind(call, null));
                assertTrue(String.valueOf(refused).contains("invalid_arguments_ref"), String.valueOf(refused));
            }

            // A plan of 400 splits is far over the 4096-byte threshold: a vendor pointer the client
            // fetches, built from the arguments resolved through the reference.
            long putsBefore = stat(vendorUrl, "puts_rpc");
            try (HttpRpcConnection conn = connection(base, alice)) {
                VgiService vgi = conn.proxy(VgiService.class);
                var request = new TableFunctionPlanRequest(Nested.of(aliceBind), null, null, null, null,
                        null, null, null, null, null, null, true, null, null, null, null, null, null, null, null);
                PlanResponse plan = vgi.table_function_plan(RecordCodec.serializeToBytes(request), null);
                assertEquals(SPLITS, plan.splits().size());
            }
            assertTrue(stat(vendorUrl, "puts_rpc") > putsBefore, "the plan result was not externalized");
            assertTrue(stat(vendorUrl, "gets_upload") >= 1, "the argument object was never fetched");
        } finally {
            stop(worker);
            stop(vendor);
            jwks.stop(0);
        }
    }

    private static HttpRpcConnection connection(String base, String pass) {
        return HttpRpcConnection.builder(base).bearerToken(pass)
                .externalLocation(ExternalLocationConfig.builder().urlValidator(uri -> { }).build())
                .requestTimeout(Duration.ofMinutes(2)).build();
    }

    private static byte[] attach(VgiService vgi) {
        var catalog = new CatalogCapabilities(List.of(), List.of(), List.of(), List.of());
        var caps = new ClientCapabilities("vgi-java-orchard-hosted-vendor-test",
                List.of(), List.of(), true, List.of(), RecordCodec.serializeToBytes(catalog));
        return vgi.catalog_attach(new CatalogAttachRequest("blob_test", new byte[0], "", "",
                RecordCodec.serializeToBytes(caps)), null).attach_opaque_data();
    }

    private static BindRequest bind(byte[] attach, ArgumentsLocation location) {
        return new BindRequest("blob_param_streams", null, "TABLE", null, null, null, attach, null, false,
                null, null, null, null, List.of("main"), null, location);
    }

    private static String pass(ECKey ec, String subject) throws Exception {
        long now = System.currentTimeMillis() / 1000;
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(ISSUER).audience(AUDIENCE).subject(subject)
                .issueTime(new Date(now * 1000)).notBeforeTime(new Date(now * 1000))
                .expirationTime(new Date((now + 900) * 1000)).jwtID("jti-" + System.nanoTime())
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(ec.getKeyID())
                .type(new JOSEObjectType("at+jwt")).build(), claims);
        jwt.sign(new ECDSASigner(ec));
        return jwt.serialize();
    }

    private static HttpResponse<String> options(String url, String token) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30))
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody());
            if (token != null) b.header("Authorization", "Bearer " + token);
            return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static String header(HttpResponse<?> r, String name) {
        return r.headers().firstValue(name).orElse("");
    }

    /** One counter from the fake vendor's {@code /__stats__}. */
    private static long stat(String vendorUrl, String name) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            String body = client.send(HttpRequest.newBuilder(URI.create(vendorUrl + "/__stats__")).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            Matcher m = Pattern.compile("\"" + name + "\"\\s*:\\s*(\\d+)").matcher(body);
            assertTrue(m.find(), name + " missing from " + body);
            return Long.parseLong(m.group(1));
        }
    }

    /** The rest of the first stdout line starting with {@code prefix}; stdout is drained after. */
    private static String readPrefixed(Process p, String prefix, int seconds) throws Exception {
        BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        String found = null;
        String line;
        while (System.nanoTime() < deadline && (line = out.readLine()) != null) {
            if (line.startsWith(prefix)) {
                found = line.substring(prefix.length()).trim();
                break;
            }
        }
        Thread drain = new Thread(() -> {
            try { while (out.readLine() != null) { /* keep the pipe empty */ } } catch (Exception ignored) { }
        });
        drain.setDaemon(true);
        drain.start();
        return found;
    }

    private static void stop(Process p) throws InterruptedException {
        if (p == null) return;
        p.descendants().forEach(ProcessHandle::destroy);
        p.destroy();
        if (!p.waitFor(15, TimeUnit.SECONDS)) p.destroyForcibly();
    }

    /** {@code java <flags> -cp <test classpath> farm.query.vgi.example.Main <args>}. */
    private static List<String> command(String... args) {
        String java = ProcessHandle.current().info().command()
                .orElse(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        List<String> cmd = new ArrayList<>(List.of(java,
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-Dio.netty.noUnsafe=false",
                "--enable-native-access=ALL-UNNAMED",
                "-Darrow.memory.debug.allocator=false"));
        cmd.addAll(List.of("-cp", System.getProperty("java.class.path"), Main.class.getName()));
        cmd.addAll(List.of(args));
        return cmd;
    }
}
