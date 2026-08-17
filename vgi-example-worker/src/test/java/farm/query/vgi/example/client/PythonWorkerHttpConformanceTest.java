// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgirpc.RpcError;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The Python leg of {@link AbstractVgiHttpConformanceTest}: the same assertions
 * run against vgi-python's reference fixture worker over HTTP.
 *
 * <p>This is the leg that carries the value. The Java client has never been
 * exercised against a worker it did not share a codebase with, so a wrong
 * shared assumption has had nowhere to show up.
 *
 * <p>It <em>skips</em> — visibly, as an aborted container with a reason, never
 * as a silent pass — when {@code uv} or a vgi-python checkout is absent, so a
 * machine with no Python side does not fail the build. Point
 * {@code VGI_PYTHON_DIR} at the checkout to run it from elsewhere; this is the
 * same gate {@code PushdownFiltersPythonConformanceTest} uses.
 */
final class PythonWorkerHttpConformanceTest extends AbstractVgiHttpConformanceTest {

    private static PythonFixtureHttpWorker worker;

    @BeforeAll
    static void startWorker() throws Exception {
        Path project = PythonFixtureHttpWorker.project();
        // Say so on stderr as well as through the assumption: Gradle's XML
        // report records an aborted container's tests as "skipped" but drops
        // the reason, and a leg that quietly contributes nothing is exactly the
        // failure mode this test exists to avoid.
        if (project == null) {
            System.err.println("[vgi-python] SKIPPING the reference-implementation leg: "
                    + "no `uv` on PATH and no vgi-python checkout (set VGI_PYTHON_DIR to point at "
                    + "one, or VGI_PYTHON_HTTP_URL at an already-running vgi-fixture-http).");
        }
        assumeTrue(project != null,
                "vgi-python + uv not available; skipping the reference-implementation leg");
        System.err.println("[vgi-python] running the reference-implementation leg (project=" + project
                + "; set VGI_PYTHON_HTTP_URL to attach to a warm server instead of spawning one).");
        worker = PythonFixtureHttpWorker.start(project);
    }

    @AfterAll
    static void stopWorker() {
        // Runs even when the assumption above aborted the container, so the
        // null guard is load-bearing rather than defensive.
        if (worker != null) worker.close();
    }

    @Override
    protected VgiHttpWorkerUnderTest worker() {
        return worker;
    }

    /**
     * A request the worker <em>rejects</em> must still come back as a protocol
     * error carrying a type and a message.
     *
     * <p>Not in the shared body because it cannot be: the trigger is a function
     * kind vgi-java's worker deliberately tolerates (see
     * {@link VgiHttpWorkerUnderTest#tableFunctionTypeLabel}), so only the
     * reference implementation rejects it. That makes this leg the only place
     * the behaviour is observable at all.
     *
     * <p>It is also the regression test for a real vgi-rpc-python defect. The
     * HTTP request-validation guard caught a fixed list of exception types and
     * let everything else escape the RPC layer, so an unrecognised member of a
     * dictionary-encoded enum — {@code KeyError} — reached Falcon and was
     * answered with its default {@code {"title": "500 Internal Server Error"}}
     * page: no Arrow body, no error type, no message, the cause visible only in
     * the server's log. Before the fix this assertion sees
     * {@code HttpError}; after it, {@code KeyError: 'table'}.
     *
     * <p>The fix lives in vgi-rpc-python, which vgi-python installs as a
     * released dependency — so until it ships, the spawned fixture still has the
     * old behaviour and this <em>skips</em>, naming what it wants. It does not
     * pass quietly: the skip fires only on the exact pre-fix shape, and any
     * other outcome is asserted. To run it against a local fix, put the checkout
     * ahead of the installed package:
     * {@code PYTHONPATH=~/Development/vgi-rpc-python ./gradlew test}.
     */
    @Test
    @Timeout(120)
    void aRejectedRequestStillCarriesATypeAndMessage() {
        RpcError rejected = assertThrows(RpcError.class,
                () -> vgi.catalog_schema_contents_functions(handle, "main", "table", null, null),
                "an unrecognised function kind must be rejected");
        assumeTrue(!isPreFixTransportError(rejected),
                "reference server predates the vgi-rpc-python fix for request-validation "
                        + "errors escaping the RPC layer; it answered: " + rejected.errorMessage());
        assertNotEquals("HttpError", rejected.errorType(),
                "a rejected request must not degrade to a transport error, got: "
                        + rejected.errorType() + " / " + rejected.errorMessage());
        assertTrue(rejected.errorMessage().contains("table"),
                "the rejection must name the value it rejected, got: " + rejected.errorMessage());
    }

    /** The exact pre-fix shape: a transport error standing in for a lost worker error. */
    private static boolean isPreFixTransportError(RpcError e) {
        return "HttpError".equals(e.errorType())
                && e.errorMessage() != null
                && e.errorMessage().contains("non-Arrow body");
    }
}
