// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.http;

import farm.query.vgi.Worker;
import farm.query.vgirpc.http.LandingInfo;

/**
 * Derives a {@link LandingInfo} from a {@link Worker}.
 *
 * <p>The shared landing page reads catalog metadata over the VGI protocol
 * through the client bundle the transport serves beside it, so the worker
 * supplies only what the protocol has no method for: which worker this is,
 * what it is called, and what version it runs.</p>
 */
public final class WorkerLandingInfo {

    private WorkerLandingInfo() {}

    /**
     * Build the landing identity for {@code worker}: name from the catalog
     * name, doc from the catalog comment's first line, version from the VGI
     * core package manifest.
     *
     * @param worker the worker to describe
     * @return its landing identity
     */
    public static LandingInfo of(Worker worker) {
        return new LandingInfo(
                worker.catalogName(),
                firstLine(worker.catalogComment()),
                packageVersion());
    }

    private static String packageVersion() {
        String v = Worker.class.getPackage() == null ? null
                : Worker.class.getPackage().getImplementationVersion();
        return v == null ? "unknown" : v;
    }

    private static String firstLine(String s) {
        if (s == null || s.isEmpty()) return "";
        int nl = s.indexOf('\n');
        return (nl < 0 ? s : s.substring(0, nl)).strip();
    }
}
