// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The vgi-python reference fixture worker, served over HTTP as a child process.
 *
 * <p>Launch recipe lifted from the C++ extension's
 * {@code test/run_http_integration.sh}, which is the known-good way to drive
 * this worker: {@code uv run --project <dir> vgi-fixture-http --port 0
 * --log-format json}, then read the {@code PORT:<n>} line it prints once bound.
 * {@code --port 0} means the port is chosen by the OS, so nothing here can
 * collide with a developer's running server.
 *
 * <p>{@link #available()} is the skip gate: no {@code uv} on PATH, or no
 * vgi-python checkout, and the Python leg opts out rather than failing a build
 * that never had a Python side. {@code VGI_PYTHON_DIR} overrides the location,
 * matching {@code PushdownFiltersPythonConformanceTest}.
 *
 * <h2>Reusing a warm server ({@code VGI_PYTHON_HTTP_URL})</h2>
 *
 * <p>A cold start is ~20 s of CPU — importing numpy and duckdb and building the
 * fixture statistics — and it is paid on every run. Point
 * {@code VGI_PYTHON_HTTP_URL} at an already-running {@code vgi-fixture-http}
 * and this harness attaches to it instead of spawning: the interpreter starts
 * once for the whole day rather than once per {@code ./gradlew test}.
 *
 * <pre>{@code
 * uv run --project ~/Development/vgi-python vgi-fixture-http --port 8080 &
 * VGI_PYTHON_HTTP_URL=http://127.0.0.1:8080 ./gradlew :vgi-example-worker:test
 * }</pre>
 *
 * <p>An attached server is <em>not</em> owned: it is health-checked and left
 * running, and neither {@code uv} nor a checkout is then required. (This is the
 * HTTP analogue of what {@code launch:} does for the C++ suite — that mechanism
 * keeps one warm worker per argv tuple, but it is AF_UNIX only and cannot front
 * an HTTP server, so the sharing has to be arranged by the caller.)
 *
 * <p>Teardown is belt-and-braces for a server this harness <em>did</em> spawn:
 * {@link #close()} destroys the process, and a shutdown hook registered at start
 * does the same if the JVM dies first. A fixture worker left listening on a
 * random port is the one failure mode of this harness that outlives the test run.
 */
final class PythonFixtureHttpWorker implements VgiHttpWorkerUnderTest {

    /** Generous: the fixture imports numpy + duckdb and precomputes stats — ~20s cold. */
    private static final long STARTUP_TIMEOUT_SECONDS = 180;

    /** How long the announced port may take to start accepting connections. */
    private static final long READY_TIMEOUT_SECONDS = 60;

    /** Env var naming an already-running fixture server to attach to. */
    private static final String REUSE_ENV = "VGI_PYTHON_HTTP_URL";

    /** The spawned worker, or {@code null} when attached to one we do not own. */
    private final Process process;
    private final String endpoint;
    private final Thread drain;
    private final Thread shutdownHook;
    private final List<String> startupLog;

    private PythonFixtureHttpWorker(Process process, String endpoint, Thread drain,
                                    Thread shutdownHook, List<String> startupLog) {
        this.process = process;
        this.endpoint = endpoint;
        this.drain = drain;
        this.shutdownHook = shutdownHook;
        this.startupLog = startupLog;
    }

    /**
     * The vgi-python checkout to run, or {@code null} when the Python leg
     * cannot run here.
     *
     * @return the project directory, or {@code null}
     */
    static Path project() {
        String configured = System.getenv("VGI_PYTHON_DIR");
        // With a server to attach to there is nothing to launch, so neither a
        // checkout nor uv is required — return a placeholder so the leg runs.
        if (reuseEndpoint() != null) {
            return configured != null && !configured.isEmpty() ? Path.of(configured) : Path.of(".");
        }
        Path project = configured != null && !configured.isEmpty()
                ? Path.of(configured)
                : Path.of(System.getProperty("user.home"), "Development", "vgi-python");
        if (!Files.isRegularFile(project.resolve("pyproject.toml"))) return null;
        return hasUv() ? project : null;
    }

    /**
     * Whether the Python leg can run in this environment.
     *
     * @return {@code true} when both {@code uv} and a vgi-python checkout exist
     */
    static boolean available() {
        return project() != null;
    }

    /**
     * Start the fixture worker and wait for it to advertise its port.
     *
     * @param project the vgi-python checkout
     * @return the running worker
     * @throws IOException          if the process cannot be launched
     * @throws InterruptedException if interrupted while waiting for the port
     */
    static PythonFixtureHttpWorker start(Path project) throws IOException, InterruptedException {
        String reuse = reuseEndpoint();
        if (reuse != null) {
            awaitReady(reuse);
            return new PythonFixtureHttpWorker(null, reuse, null, null, List.of());
        }
        // Errors are merged into stdout so a startup failure shows up in the
        // same buffer we're already reading the PORT line out of, rather than
        // filling an unread pipe and deadlocking the child.
        Process p = new ProcessBuilder(
                "uv", "run", "--project", project.toString(),
                "vgi-fixture-http", "--port", "0", "--log-format", "json")
                .redirectErrorStream(true)
                .start();

        Thread hook = new Thread(p::destroyForcibly, "vgi-python-fixture-reaper");
        Runtime.getRuntime().addShutdownHook(hook);

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        List<String> log = new ArrayList<>();
        int port = -1;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STARTUP_TIMEOUT_SECONDS);
        try {
            String line;
            while (System.nanoTime() < deadline && (line = reader.readLine()) != null) {
                log.add(line);
                int at = line.indexOf("PORT:");
                if (at >= 0) {
                    port = Integer.parseInt(line.substring(at + "PORT:".length()).trim());
                    break;
                }
            }
        } catch (IOException e) {
            log.add("(read failed: " + e + ")");
        }
        if (port <= 0) {
            Runtime.getRuntime().removeShutdownHook(hook);
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
            throw new IllegalStateException(
                    "vgi-fixture-http never printed a PORT: line. Output was:\n"
                            + String.join("\n", log));
        }

        // Keep draining after startup: waitress logs per request, and a full
        // pipe would block the worker mid-test. Anything that looks like a
        // worker-side failure is echoed — an HTTP 500 reaches the client as an
        // opaque error object, and the traceback that explains it only ever
        // exists here.
        Thread drain = new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("Traceback") || line.contains("\"level\": \"ERROR\"")
                            || line.contains("Error") || line.contains("Exception")) {
                        System.err.println("[vgi-python] " + line);
                    }
                }
            } catch (IOException ignore) {
                // Closed on teardown — expected.
            }
        }, "vgi-python-fixture-drain");
        drain.setDaemon(true);
        drain.start();

        String endpoint = "http://127.0.0.1:" + port;
        awaitReady(endpoint);
        return new PythonFixtureHttpWorker(p, endpoint, drain, hook, log);
    }

    @Override public String endpoint() { return endpoint; }

    @Override public String label() { return "vgi-python"; }

    /** vgi-python emits the FunctionType enum's <em>name</em> here. */
    @Override public String tableFunctionTypeLabel() { return "TABLE"; }

    /** {@return the lines the worker printed before announcing its port} */
    List<String> startupLog() { return List.copyOf(startupLog); }

    @Override
    public void close() {
        if (process == null) return; // Attached, not owned — leave it running.
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignore) {
            // Already shutting down; the hook will do the killing.
        }
        process.destroy();
        try {
            if (!process.waitFor(15, TimeUnit.SECONDS)) process.destroyForcibly();
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
        drain.interrupt();
    }

    /**
     * Block until the worker actually accepts connections.
     *
     * <p>The {@code PORT:} line is printed as soon as the port is <em>chosen</em>,
     * which is before waitress is serving on it — so connecting the instant the
     * line appears intermittently gets a refusal. vgi-python's own harness has
     * the same two-step (see {@code tests/_http_fixtures.py}:
     * {@code start_http_worker} scrapes the port and then calls
     * {@code wait_for_http_server}); this is that second step.
     */
    private static void awaitReady(String endpoint) throws InterruptedException {
        HttpRequest probe = HttpRequest.newBuilder(URI.create(endpoint + "/health"))
                .timeout(Duration.ofSeconds(5)).GET().build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(READY_TIMEOUT_SECONDS);
        Exception last = null;
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2)).build()) {
            while (System.nanoTime() < deadline) {
                try {
                    HttpResponse<Void> r = client.send(probe, HttpResponse.BodyHandlers.discarding());
                    if (r.statusCode() == 200) return;
                } catch (IOException e) {
                    last = e;
                }
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException(
                "vgi-fixture-http printed a port but never became reachable at " + endpoint,
                last);
    }

    /** The server to attach to, or {@code null} to spawn one. */
    private static String reuseEndpoint() {
        String url = System.getenv(REUSE_ENV);
        if (url == null || url.isBlank()) return null;
        String trimmed = url.trim();
        while (trimmed.endsWith("/")) trimmed = trimmed.substring(0, trimmed.length() - 1);
        return trimmed;
    }

    private static boolean hasUv() {
        try {
            Process p = new ProcessBuilder("uv", "--version").redirectErrorStream(true).start();
            boolean done = p.waitFor(30, TimeUnit.SECONDS);
            if (!done) {
                p.destroyForcibly();
                return false;
            }
            return p.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }
}
