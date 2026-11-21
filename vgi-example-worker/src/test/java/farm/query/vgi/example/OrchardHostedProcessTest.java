// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import farm.query.vgi.VgiService;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.ClientCapabilities;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpRpcStream;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Orchard hosted mode ({@code VGI_ORCHARD_HOSTED=1}), end to end -- the Java port of vgi-python's
 * {@code tests/test_orchard_hosted.py}.
 *
 * <p>Runs the example worker ({@link Main}) as a real process in hosted mode against a local JWKS
 * server holding a freshly generated ES256 key, then checks the contract:
 * <ul>
 *   <li>a valid Pass reaches function code ({@code pass_claims}), with claims in their JSON types;</li>
 *   <li>no Pass, a tampered claim, the wrong {@code aud}, several audiences, an expired Pass, the
 *       wrong {@code typ}, RS256 / HS256 tokens, the wrong {@code iss} and no {@code exp} all get 401
 *       with {@code WWW-Authenticate: Bearer error="invalid_token"}, the capability headers and no
 *       {@code resource_metadata}; {@code HEAD /health} alone is anonymous;</li>
 *   <li>the {@code Authorization} header never reaches the logs, even at debug level;</li>
 *   <li>every misconfiguration refuses to start, with a non-zero exit.</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
final class OrchardHostedProcessTest {

    private static final String ISSUER = "https://auth.orchard.test";
    private static final String AUDIENCE = "https://desk.orchard.test/d/dep_123";
    private static final String CHALLENGE = "Bearer error=\"invalid_token\"";
    private static final String UNUSED_JWKS = "http://127.0.0.1:9/jwks"; // never contacted: startup refuses first

    private static ECKey ec;
    private static RSAKey rsa;
    private static com.sun.net.httpserver.HttpServer jwks;
    private static String jwksUri;
    private static Process worker;
    private static String base;
    private static Path log;
    private static final List<String> USED_TOKENS = new CopyOnWriteArrayList<>();

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    @BeforeAll
    static void startWorker() throws Exception {
        ec = new ECKeyGenerator(Curve.P_256).keyID("desk-es256-1").generate();
        rsa = new RSAKeyGenerator(2048).keyID("desk-rs256-1").generate();
        byte[] body = new JWKSet(List.of(ec.toPublicJWK(), rsa.toPublicJWK())).toString()
                .getBytes(StandardCharsets.UTF_8);
        jwks = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jwks.createContext("/jwks", ex -> {
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.getResponseBody().close();
        });
        jwks.start();
        jwksUri = "http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks";

        log = Files.createTempFile("orchard-hosted-worker", ".log");
        ProcessBuilder pb = new ProcessBuilder(command(List.of(
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"), "--http", "--host", "127.0.0.1", "--port", "0"));
        scrub(pb.environment());
        pb.environment().putAll(env(jwksUri, Map.of()));
        pb.redirectError(log.toFile());
        worker = pb.start();
        BufferedReader out = new BufferedReader(new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        Integer port = null;
        String line;
        while (System.nanoTime() < deadline && (line = out.readLine()) != null) {
            if (line.startsWith("PORT:")) {
                port = Integer.parseInt(line.substring(5).trim());
                break;
            }
        }
        if (port == null) {
            worker.destroyForcibly();
            fail("hosted worker did not start:\n" + Files.readString(log));
        }
        Thread drain = new Thread(() -> {
            try { while (out.readLine() != null) { /* keep the pipe empty */ } } catch (Exception ignored) { }
        });
        drain.setDaemon(true);
        drain.start();
        base = "http://127.0.0.1:" + port;
    }

    @AfterAll
    static void stopWorker() throws Exception {
        if (worker != null) {
            worker.destroy();
            if (!worker.waitFor(15, TimeUnit.SECONDS)) worker.destroyForcibly();
        }
        if (jwks != null) jwks.stop(0);
    }

    /** {@code java <jvm flags> -cp <test classpath> farm.query.vgi.example.Main <args>}. */
    private static List<String> command(List<String> jvmFlags, String... args) {
        String java = ProcessHandle.current().info().command()
                .orElse(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        List<String> cmd = new ArrayList<>(List.of(java,
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-Dio.netty.noUnsafe=false",
                "--enable-native-access=ALL-UNNAMED",
                "-Darrow.memory.debug.allocator=false"));
        cmd.addAll(jvmFlags);
        cmd.addAll(List.of("-cp", System.getProperty("java.class.path"), Main.class.getName()));
        cmd.addAll(List.of(args));
        return cmd;
    }

    /** Remove inherited auth settings that would make the worker refuse (or misbehave). */
    private static void scrub(Map<String, String> env) {
        for (String k : List.of("VGI_SIGNING_KEY_MINTED", "VGI_BEARER_TOKENS", "VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE",
                "VGI_JWT_JWKS_URI", "VGI_JWT_ALGORITHMS", "VGI_RPC_GRANT_KEYS", "VGI_OAUTH_RESOURCE",
                "VGI_OAUTH_AUTH_SERVERS", "VGI_PROXY_PROOF_MODE", "VGI_INTROSPECT_PRINCIPALS",
                "VGI_ORCHARD_HOSTED", "VGI_TEST_BEARER_TOKEN", "VGI_FIXTURE_IDENTITY", "VGI_WORKER_CATALOG_NAME",
                "VGI_WORKER_STDERR")) {
            env.remove(k);
        }
    }

    /** The hosted environment, with {@code overrides} applied ({@code null} removes a variable). */
    private static Map<String, String> env(String jwksUri, Map<String, String> overrides) {
        Map<String, String> env = new HashMap<>();
        env.put("VGI_ORCHARD_HOSTED", "1");
        env.put("VGI_JWT_ISSUER", ISSUER);
        env.put("VGI_JWT_AUDIENCE", AUDIENCE);
        env.put("VGI_JWT_JWKS_URI", jwksUri);
        env.put("VGI_SIGNING_KEY", "orchard-hosted-test-signing-key-0001");
        env.putAll(overrides);
        return env;
    }

    // ------------------------------------------------------------------
    // Passes
    // ------------------------------------------------------------------

    private static String pass(Map<String, Object> overrides) throws Exception {
        return pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), "at+jwt", overrides);
    }

    private static String pass(JWSSigner signer, JWSAlgorithm alg, String kid, String typ,
                               Map<String, Object> overrides) throws Exception {
        long now = System.currentTimeMillis() / 1000;
        Map<String, Object> claims = new HashMap<>();
        claims.put("iss", ISSUER);
        claims.put("aud", AUDIENCE);
        claims.put("sub", "user_01M7KNJYC18T");
        claims.put("kind", "user");
        claims.put("email", "ada@example.com");
        claims.put("email_verified", true);
        claims.put("name", "Ada Lovelace");
        claims.put("org_id", "org_01M7");
        claims.put("org_name", "Acme Analytics");
        claims.put("client_id", "vgi-duckdb");
        claims.put("iat", now);
        claims.put("nbf", now);
        claims.put("exp", now + 900);
        claims.put("jti", "jti-" + System.nanoTime());
        claims.putAll(overrides);
        JWTClaimsSet.Builder b = new JWTClaimsSet.Builder();
        claims.forEach((k, v) -> {
            if (v == null) return;
            switch (k) {
                case "iat" -> b.issueTime(new Date(((Number) v).longValue() * 1000));
                case "nbf" -> b.notBeforeTime(new Date(((Number) v).longValue() * 1000));
                case "exp" -> b.expirationTime(new Date(((Number) v).longValue() * 1000));
                default -> b.claim(k, v);
            }
        });
        SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(alg).keyID(kid).type(new JOSEObjectType(typ)).build(),
                b.build());
        jwt.sign(signer);
        return jwt.serialize();
    }

    private static String tamper(String token, String claim, String value) {
        String[] parts = token.split("\\.");
        String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        String forged = payload.replaceFirst("\"" + claim + "\":\"[^\"]*\"", "\"" + claim + "\":\"" + value + "\"");
        assertNotEquals(payload, forged);
        return parts[0] + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(forged.getBytes(StandardCharsets.UTF_8)) + "." + parts[2];
    }

    private static String use(String token) {
        USED_TOKENS.add(token);
        return token;
    }

    private static HttpResponse<String> request(String method, String path, String token,
                                                Map<String, String> headers) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path))
                    .timeout(Duration.ofSeconds(30))
                    .method(method, "POST".equals(method)
                            ? HttpRequest.BodyPublishers.ofByteArray(new byte[0])
                            : HttpRequest.BodyPublishers.noBody());
            if (token != null) b.header("Authorization", "Bearer " + token);
            headers.forEach(b::header);
            return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    /** {@code pass_claims(1)} over the real VGI protocol, presenting {@code token}. */
    private static String passClaims(String token) {
        try (HttpRpcConnection conn = HttpRpcConnection.builder(base).bearerToken(token)
                .requestTimeout(Duration.ofMinutes(2)).build()) {
            VgiService vgi = conn.proxy(VgiService.class);
            var catalog = new CatalogCapabilities(List.of(), List.of(), List.of(), List.of());
            var caps = new ClientCapabilities("vgi-java-orchard-hosted-test",
                    List.of(), List.of(), true, List.of(), RecordCodec.serializeToBytes(catalog));
            byte[] handle = vgi.catalog_attach(new CatalogAttachRequest("example", new byte[0], "", "",
                    RecordCodec.serializeToBytes(caps)), null).attach_opaque_data();
            Schema input = new Schema(List.of(Field.nullable("x", new ArrowType.Int(64, true))));
            BindRequest bind = new BindRequest(
                    "pass_claims", ArgumentsEncoder.builder().encode(), "SCALAR",
                    SchemaUtil.serializeSchema(input), SettingsEncoder.builder().encode(), null,
                    handle, null, false, null, null, null, null, "main");
            BindResponse bound = vgi.bind(bind, null);
            RpcStream<? extends StreamState> stream = vgi.init(new InitRequest(
                    RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                    null, null, null, null, null, null,
                    null, null, null, null, null, null, null, null, null, null), null);
            HttpRpcStream<?> session = (HttpRpcStream<?>) stream;
            try (VectorSchemaRoot in = VectorSchemaRoot.create(input, Allocators.root())) {
                in.allocateNew();
                ((BigIntVector) in.getVector("x")).setSafe(0, 1L);
                in.getVector("x").setValueCount(1);
                in.setRowCount(1);
                AnnotatedBatch answer = session.exchange(new AnnotatedBatch(in, null));
                return new String(((VarCharVector) answer.root().getVector("result")).get(0), StandardCharsets.UTF_8);
            } finally {
                session.close();
            }
        }
    }

    private static void assert401(HttpResponse<String> r) {
        assertEquals(401, r.statusCode(), r.body());
        String challenge = r.headers().firstValue("WWW-Authenticate").orElse("");
        assertEquals(CHALLENGE, challenge);
        assertFalse(challenge.contains("resource_metadata"));
        assertTrue(r.headers().firstValue("VGI-Supported-Encodings").isPresent(), "capability headers on the 401");
        assertTrue(r.headers().firstValue("VGI-Accept-Max-Response-Bytes-Support").isPresent()
                || r.headers().firstValue("VGI-Externalization-Enabled").isPresent());
        String reason = r.headers().firstValue("VGI-Auth-Reason").orElse("");
        assertTrue(List.of("missing_credential", "invalid_credential", "expired_credential").contains(reason), reason);
    }

    // ------------------------------------------------------------------
    // A running hosted worker
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    @Timeout(120)
    void validPassReachesFunctionWithJsonTypes() throws Exception {
        String json = passClaims(use(pass(Map.of("name", "Ada Lovelace \u2013 Zo\u00eb \u2713",
                "federated_iss", "https://accounts.google.com"))));
        assertTrue(json.startsWith("{\"claims\": {\"aud\": \"" + AUDIENCE + "\", \"client_id\": \"vgi-duckdb\", "
                + "\"email\": \"ada@example.com\", \"email_verified\": true, \"exp\": " + extractExp(json) + ", "
                + "\"federated_iss\": \"https://accounts.google.com\", \"iat\": "), json);
        assertTrue(json.contains("\"name\": \"Ada Lovelace \\u2013 Zo\\u00eb \\u2713\""), json);
        assertTrue(json.endsWith(", \"org_id\": \"org_01M7\", \"org_name\": \"Acme Analytics\", "
                + "\"sub\": \"user_01M7KNJYC18T\"}, \"domain\": \"orchard\", \"principal\": \"user_01M7KNJYC18T\"}"),
                json);
    }

    private static String extractExp(String json) {
        Matcher m = Pattern.compile("\"exp\": (\\d+)[,}]").matcher(json);
        assertTrue(m.find(), "exp is a JSON integer: " + json);
        return m.group(1);
    }

    @Test
    @Order(2)
    void headHealthIsAnonymous() throws Exception {
        HttpResponse<String> r = request("HEAD", "/health", null, Map.of());
        assertEquals(200, r.statusCode());
        assertTrue(r.headers().firstValue("VGI-Supported-Encodings").isPresent());
    }

    @ParameterizedTest
    @Order(3)
    @ValueSource(strings = {"GET /health", "OPTIONS /health", "GET /", "POST /vgi.v3/catalog_attach",
            "GET /describe.json", "GET /no/such/path"})
    void noPass401(String route) throws Exception {
        String[] mp = route.split(" ");
        HttpResponse<String> r = request(mp[0], mp[1], null, Map.of("Origin", "https://shell.example.com"));
        assert401(r);
        assertEquals("missing_credential", r.headers().firstValue("VGI-Auth-Reason").orElse(""));
        assertEquals("*", r.headers().firstValue("Access-Control-Allow-Origin").orElse(""));
    }

    @ParameterizedTest
    @Order(4)
    @ValueSource(strings = {"garbage", "tampered", "wrong_aud", "multi_aud", "expired", "wrong_typ", "rs256",
            "hs256", "wrong_iss", "no_exp", "not_bearer"})
    void badPass401(String kind) throws Exception {
        long now = System.currentTimeMillis() / 1000;
        Map<String, String> headers = new HashMap<>(Map.of("Origin", "https://shell.example.com"));
        String token = switch (kind) {
            case "garbage" -> "not-a-jwt";
            case "tampered" -> tamper(pass(Map.of()), "org_id", "org_EVIL");
            case "wrong_aud" -> pass(Map.of("aud", "https://desk.orchard.test/d/dep_other"));
            case "multi_aud" -> pass(Map.of("aud", List.of(AUDIENCE, "https://desk.orchard.test/d/dep_other")));
            case "expired" -> pass(Map.of("iat", now - 1200, "nbf", now - 1200, "exp", now - 120));
            case "wrong_typ" -> pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), "JWT", Map.of());
            case "rs256" -> pass(new RSASSASigner(rsa), JWSAlgorithm.RS256, rsa.getKeyID(), "at+jwt", Map.of());
            case "hs256" -> pass(new MACSigner(new byte[32]), JWSAlgorithm.HS256, ec.getKeyID(), "at+jwt", Map.of());
            case "wrong_iss" -> pass(Map.of("iss", "https://evil.test"));
            case "no_exp" -> {
                Map<String, Object> o = new HashMap<>();
                o.put("exp", null);
                yield pass(o);
            }
            case "not_bearer" -> {
                headers.put("Authorization", "Basic " + Base64.getEncoder().encodeToString("a:b".getBytes()));
                yield null;
            }
            default -> throw new IllegalArgumentException(kind);
        };
        HttpResponse<String> r = request("POST", "/vgi.v3/catalog_attach", token == null ? null : use(token), headers);
        assert401(r);
        assertEquals("expired".equals(kind) ? "expired_credential" : "invalid_credential",
                r.headers().firstValue("VGI-Auth-Reason").orElse(""));
    }

    @Test
    @Order(5)
    void preflightOpenCors() throws Exception {
        HttpResponse<String> r = request("OPTIONS", "/vgi.v3/catalog_attach", null, Map.of(
                "Origin", "https://shell.example.com",
                "Access-Control-Request-Method", "POST",
                "Access-Control-Request-Headers", "authorization"));
        assertEquals(200, r.statusCode());
        assertEquals("*", r.headers().firstValue("Access-Control-Allow-Origin").orElse(""));
    }

    @Test
    @Order(99)
    @Timeout(120)
    void authorizationNeverLogged() throws Exception {
        passClaims(use(pass(Map.of())));
        for (String bad : List.of(tamper(pass(Map.of()), "sub", "mallory"),
                pass(Map.of("aud", "https://other.test")),
                pass(new ECDSASigner(ec), JWSAlgorithm.ES256, ec.getKeyID(), "JWT", Map.of()))) {
            assertEquals(401, request("POST", "/vgi.v3/catalog_attach", use(bad), Map.of()).statusCode());
        }
        Thread.sleep(500);
        String text = Files.readString(log);
        assertTrue(text.contains("DEBUG"), "expected the worker's debug log to be captured:\n" + text);
        for (String token : USED_TOKENS) {
            assertFalse(text.contains(token), "a token reached the log");
            String[] parts = token.split("\\.");
            if (parts.length == 3) assertFalse(text.contains(parts[2]), "a token signature reached the log");
        }
        assertFalse(text.contains("Bearer "), "an Authorization value reached the log");
    }

    // ------------------------------------------------------------------
    // Startup refusals
    // ------------------------------------------------------------------

    /** Run the worker expecting a refusal; returns its stderr. */
    private static String refused(Map<String, String> overrides, String... args) throws Exception {
        List<String> argv = args.length == 0
                ? List.of("--http", "--host", "127.0.0.1", "--port", "0") : List.of(args);
        ProcessBuilder pb = new ProcessBuilder(command(List.of(), argv.toArray(new String[0])));
        scrub(pb.environment());
        Map<String, String> env = env(UNUSED_JWKS, Map.of());
        env.putAll(overrides);
        env.forEach((k, v) -> {
            if (v == null) pb.environment().remove(k);
            else pb.environment().put(k, v);
        });
        Path err = Files.createTempFile("orchard-hosted-refusal", ".err");
        pb.redirectError(err.toFile());
        Process p = pb.start();
        String stdout = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(90, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            fail("worker neither started nor refused within 90 s");
        }
        String stderr = Files.readString(err);
        assertNotEquals(0, p.exitValue(), "worker started when it should have refused:\n" + stdout + "\n" + stderr);
        assertFalse(stdout.contains("PORT:"), stdout);
        return stderr;
    }

    private static Map<String, String> with(String k, String v) {
        Map<String, String> m = new HashMap<>();
        m.put(k, v);
        return m;
    }

    @ParameterizedTest
    @ValueSource(strings = {"VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"})
    void unset(String name) throws Exception {
        assertTrue(refused(with(name, null)).contains(name));
    }

    @ParameterizedTest
    @ValueSource(strings = {"VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"})
    void empty(String name) throws Exception {
        assertTrue(refused(with(name, "  ")).contains(name));
    }

    @Test
    void signingKeyUnset() throws Exception {
        assertTrue(refused(with("VGI_SIGNING_KEY", null)).contains("VGI_SIGNING_KEY"));
    }

    @Test
    void signingKeyShort() throws Exception {
        String stderr = refused(with("VGI_SIGNING_KEY", "short-key"));
        assertTrue(stderr.contains("VGI_SIGNING_KEY") && stderr.contains("32 bytes"), stderr);
    }

    @Test
    void signingKeyMinted() throws Exception {
        Map<String, String> o = with("VGI_SIGNING_KEY", "minted-by-a-parent-vgi-serve-process-0001");
        o.put("VGI_SIGNING_KEY_MINTED", "1");
        assertTrue(refused(o).contains("VGI_SIGNING_KEY"));
    }

    @Test
    void commaAudience() throws Exception {
        String stderr = refused(with("VGI_JWT_AUDIENCE", AUDIENCE + ",https://desk.orchard.test/d/other"));
        assertTrue(stderr.contains("VGI_JWT_AUDIENCE") && stderr.contains("exactly one"), stderr);
    }

    @Test
    void bearerTokens() throws Exception {
        assertTrue(refused(with("VGI_BEARER_TOKENS", "tok=alice")).contains("VGI_BEARER_TOKENS"));
    }

    @Test
    void grantKeysEnv() throws Exception {
        String key = Base64.getEncoder().encodeToString(new byte[32]);
        assertTrue(refused(with("VGI_RPC_GRANT_KEYS", key)).contains("VGI_RPC_GRANT_KEYS"));
    }

    @Test
    void grantKeysFlag() throws Exception {
        String key = Base64.getEncoder().encodeToString(new byte[32]);
        assertTrue(refused(Map.of(), "--http", "--port", "0", "--grant-key", key).toLowerCase().contains("grant"));
    }

    @Test
    void resolveTokenHook() throws Exception {
        // --identity installs Worker.resolveToken (the vgi-rpc conformance identity fixture).
        Map<String, String> o = with("VGI_FIXTURE_IDENTITY", "1");
        o.put("VGI_INTROSPECT_PRINCIPALS", "ops");
        assertTrue(refused(o).contains("resolve_token"));
    }

    @Test
    void oauthResource() throws Exception {
        Map<String, String> o = with("VGI_OAUTH_RESOURCE", "https://x.test");
        o.put("VGI_OAUTH_AUTH_SERVERS", ISSUER);
        assertTrue(refused(o).contains("VGI_OAUTH_RESOURCE"));
    }

    @Test
    void proxyProofMode() throws Exception {
        assertTrue(refused(with("VGI_PROXY_PROOF_MODE", "require")).contains("VGI_PROXY_PROOF_MODE"));
    }

    @Test
    void hsAlgorithmOverride() throws Exception {
        assertTrue(refused(with("VGI_JWT_ALGORITHMS", "ES256,HS256")).contains("VGI_JWT_ALGORITHMS"));
    }

    @Test
    void unrecognisedFlagValue() throws Exception {
        assertTrue(refused(with("VGI_ORCHARD_HOSTED", "yes please")).contains("VGI_ORCHARD_HOSTED"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"--tcp 127.0.0.1:0", "--unix /tmp/vgi-java-orchard-hosted-refuse.sock",
            "--iroh-raw-upstream 127.0.0.1:0 --iroh-issuer x", "stdio"})
    void unauthenticatedTransports(String flags) throws Exception {
        String[] args = "stdio".equals(flags) ? new String[0] : flags.split(" ");
        String stderr = args.length == 0 ? refusedStdio() : refused(Map.of(), args);
        assertTrue(stderr.contains("only --http"), stderr);
    }

    /** No transport flag means stdin/stdout, which must refuse too (stdin closed at once). */
    private static String refusedStdio() throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command(List.of()));
        scrub(pb.environment());
        pb.environment().putAll(env(UNUSED_JWKS, Map.of()));
        Process p = pb.start();
        p.getOutputStream().close();
        String stderr = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(90, TimeUnit.SECONDS));
        assertNotEquals(0, p.exitValue());
        return stderr;
    }
}
