package farm.query.vgi.catalog.snapshot;
import farm.query.vgirpc.external.ExternalRef;
import farm.query.vgirpc.external.ExternalStorage;
/** Trusted discovery plus immutable storage. Implementations must make manifest mutations atomic. */
public interface CatalogPublicationRepository {
    String profile();
    ExternalStorage storage();
    CatalogPublicationManifest lookup(String key) throws Exception;
    CatalogPublicationManifest commitIfAbsent(CatalogPublicationManifest manifest) throws Exception;
    boolean invalidate(String key, String manifestId) throws Exception;
    PublicationLocation location(ExternalRef reference) throws Exception;
    /** Persist renewed references conditionally; never shorten an issued object's retention. */
    default CatalogPublicationManifest renew(CatalogPublicationManifest manifest) throws Exception { return null; }
}
