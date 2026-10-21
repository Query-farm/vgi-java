package farm.query.vgi.catalog.snapshot;
import farm.query.vgirpc.external.ExternalRef;
/** Trusted discovery descriptor. Its URL may carry credentials and must not be logged. */
public record CatalogPublicationManifest(String cache_key, String manifest_id, String protocol_name,
        String wire_revision, String contract_hash, String codec_revision, long catalog_version, String etag,
        String sha256, long raw_size, long stored_size, String url, String object_id, Double expires_at,
        Double retain_until, boolean stable, int format_version, int validator_revision) {
    public ExternalRef reference() { return new ExternalRef(url, sha256); }
    @Override public String toString() { return "CatalogPublicationManifest[" + cache_key + "]"; }
}
