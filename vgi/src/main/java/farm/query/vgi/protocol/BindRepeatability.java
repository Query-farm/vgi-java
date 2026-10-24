// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;

/** Promise for equivalent scans from the same complete bind state within one query.
 * This does not promise a snapshot across independent binds, ordering, retries or cross-query caching. */
public enum BindRepeatability {
    UNKNOWN,
    STABLE_WITHIN_QUERY
}
