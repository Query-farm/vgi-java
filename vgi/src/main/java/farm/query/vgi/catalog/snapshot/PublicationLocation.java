package farm.query.vgi.catalog.snapshot;
/** Uploader lifetime guarantees, as Unix epoch seconds. Unknown expiry is not a stable location. */
public record PublicationLocation(String object_id, Double expires_at, Double retain_until, boolean stable) {}
