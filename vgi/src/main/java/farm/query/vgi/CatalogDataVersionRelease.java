// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import java.time.Instant;

/**
 * One published data version of a catalog, surfaced through
 * {@code catalog_catalogs()} so clients can render a release timeline before
 * attaching. Mirrors vgi-python {@code CatalogDataVersionRelease}.
 *
 * <p>The {@code CatalogInfo.releases} list this belongs to is ordered
 * newest-first, with at most one entry per {@code version}.</p>
 *
 * @param version    concrete published version (e.g. {@code "1.1.0"}), not a spec
 * @param releasedAt release timestamp (UTC). The wire column is nullable, but
 *                   this SDK requires a value rather than publishing a
 *                   release with no date
 * @param summary    one-line human summary (empty string when unknown)
 * @param notesUrl   optional per-release link to detailed notes; may be null
 */
public record CatalogDataVersionRelease(
        String version,
        Instant releasedAt,
        String summary,
        String notesUrl) {

    /**
     * Enforces this SDK's stricter-than-wire invariant: {@code version} and
     * {@code releasedAt} must be present, and a {@code null} summary is
     * normalised to the empty string ({@code notesUrl} alone may stay null).
     */
    public CatalogDataVersionRelease {
        if (version == null || version.isEmpty()) {
            throw new IllegalArgumentException("CatalogDataVersionRelease.version is required");
        }
        if (releasedAt == null) {
            throw new IllegalArgumentException("CatalogDataVersionRelease.releasedAt is required");
        }
        if (summary == null) summary = "";
    }
}
