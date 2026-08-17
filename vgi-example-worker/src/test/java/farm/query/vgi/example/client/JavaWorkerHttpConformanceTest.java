// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * The Java leg of {@link AbstractVgiHttpConformanceTest}: the shared assertions
 * run against vgi-java's own example worker over HTTP.
 *
 * <p>Always runs — it needs nothing beyond this build — so a green Python leg
 * and a green Java leg together are what prove agreement, and a red one alone
 * names the side that is wrong.
 */
final class JavaWorkerHttpConformanceTest extends AbstractVgiHttpConformanceTest {

    private static EmbeddedJavaHttpWorker worker;

    @BeforeAll
    static void startWorker() throws Exception {
        worker = EmbeddedJavaHttpWorker.start();
    }

    @AfterAll
    static void stopWorker() {
        if (worker != null) worker.close();
    }

    @Override
    protected VgiHttpWorkerUnderTest worker() {
        return worker;
    }
}
