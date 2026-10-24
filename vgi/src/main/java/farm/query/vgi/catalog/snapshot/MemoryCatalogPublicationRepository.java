package farm.query.vgi.catalog.snapshot;
import farm.query.vgirpc.external.ExternalRef;
import farm.query.vgirpc.external.ExternalStorage;
import java.util.concurrent.ConcurrentHashMap;
/** In-process discovery; use a durable repository for reuse after worker restart. */
public final class MemoryCatalogPublicationRepository implements CatalogPublicationRepository {
    private final ExternalStorage storage; private final String profile; private final PublicationLocation location;
    private final ConcurrentHashMap<String, CatalogPublicationManifest> entries = new ConcurrentHashMap<>();
    public MemoryCatalogPublicationRepository(ExternalStorage storage, String profile, PublicationLocation location) { this.storage=storage; this.profile=profile; this.location=location; }
    public String profile() { return profile; }
    public ExternalStorage storage() { return storage; }
    public CatalogPublicationManifest lookup(String key) { return entries.get(key); }
    public CatalogPublicationManifest commitIfAbsent(CatalogPublicationManifest manifest) { return entries.computeIfAbsent(manifest.cache_key(), ignored -> manifest); }
    public boolean invalidate(String key, String id) { var old=entries.get(key); return old != null && old.manifest_id().equals(id) && entries.remove(key,old); }
    public PublicationLocation location(ExternalRef reference) { return location; }
}
