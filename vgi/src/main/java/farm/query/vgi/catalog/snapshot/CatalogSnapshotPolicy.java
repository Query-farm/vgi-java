package farm.query.vgi.catalog.snapshot;
import farm.query.vgirpc.external.ExternalLocationConfig;
import java.util.function.Function;
/** Opt-in catalog publication. Omit the policy to retain ordinary delivery. */
public final class CatalogSnapshotPolicy {
    public final CatalogPublicationRepository repository;
    public final Function<SnapshotRequest, SnapshotIdentity> identity;
    public final ExternalLocationConfig external;
    public Function<SnapshotRequest,String> validator;
    public boolean ordinaryOnFailure = false;
    public long maxRawBytes = 128L*1024*1024, maxStoredBytes = 128L*1024*1024;
    public double expiryMarginSeconds = 60;
    public int maxCachedManifests = 256;
    public final CatalogSnapshotManager manager = new CatalogSnapshotManager();
    public CatalogSnapshotPolicy(CatalogPublicationRepository repository, Function<SnapshotRequest, SnapshotIdentity> identity, ExternalLocationConfig external) {
        this.repository=java.util.Objects.requireNonNull(repository); this.identity=java.util.Objects.requireNonNull(identity); this.external=java.util.Objects.requireNonNull(external);
    }
}
