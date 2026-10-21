package farm.query.vgi.catalog.snapshot;

import com.google.gson.Gson;
import farm.query.vgi.CatalogContents;
import farm.query.vgi.MetadataCollections;
import farm.query.vgi.protocol.*;
import farm.query.vgirpc.external.*;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Supplier;

/** Builds, publishes and validates once; warm calls retain only a small descriptor. */
public final class CatalogSnapshotManager {
    public record Delivery(CatalogContentsResponse response, ExternalRef reference) {}
    private static final String WIRE="2.2", CODEC="catalog-collections-1";
    private static final Gson JSON=new Gson();
    private static final Schema OUTER=new Schema(List.of(new Field("result",FieldType.notNullable(ArrowType.Binary.INSTANCE),List.of())));
    private final Map<String,CatalogPublicationManifest> validated=new java.util.concurrent.ConcurrentHashMap<>();
    private static final class Gate { final java.util.concurrent.locks.ReentrantLock lock=new java.util.concurrent.locks.ReentrantLock();int references; }
    private final java.util.concurrent.ConcurrentHashMap<String,Gate> gates=new java.util.concurrent.ConcurrentHashMap<>();
    private final class GateLease implements AutoCloseable {
        final String key;final Gate gate;
        GateLease(String key,Gate gate){this.key=key;this.gate=gate;gate.lock.lock();}
        public void close(){gate.lock.unlock();gates.compute(key,(ignored,current)->--current.references==0?null:current);}
    }
    private GateLease lock(String key){var gate=gates.compute(key,(ignored,current)->{if(current==null)current=new Gate();current.references++;return current;});return new GateLease(key,gate);}


    public Delivery serve(CatalogSnapshotPolicy policy, SnapshotRequest request, String ifNoneMatch, Supplier<CatalogContentsResponse> build) {
        if(policy.maxRawBytes<=0||policy.maxStoredBytes<=0||policy.maxCachedManifests<=0||!Double.isFinite(policy.expiryMarginSeconds)||policy.expiryMarginSeconds<0) throw new IllegalArgumentException("invalid catalog snapshot policy limits");
        if (policy.validator != null) {
            String etag=policy.validator.apply(request);
            if (etag != null && etag.equals(ifNoneMatch)) return new Delivery(CatalogContents.unchanged(request.catalog_version(),etag),null);
        }
        try { return serveChecked(policy,request,ifNoneMatch,build); }
        catch (Exception error) {
            if (policy.ordinaryOnFailure) {
                SnapshotIdentity before=policy.identity.apply(request);
                CatalogContentsResponse response=build.get();
                CatalogContents.schemas(response);
                if (before.equals(policy.identity.apply(request)) && before.catalog_version()==response.catalog_version()) return new Delivery(response.etag()!=null&&response.etag().equals(ifNoneMatch)?CatalogContents.unchanged(response.catalog_version(),response.etag()):response,null);
            }
            throw new IllegalStateException("catalog publication failed",error);
        }
    }

    private Delivery serveChecked(CatalogSnapshotPolicy p, SnapshotRequest request, String inm, Supplier<CatalogContentsResponse> build) throws Exception {
        for (int attempt=0;attempt<3;attempt++) {
            SnapshotIdentity id=p.identity.apply(request);
            String key=key(p,request,id);
            try(var lease=lock(key)) {
            CatalogPublicationManifest manifest=validated.get(key);
            if (manifest==null) {
                CatalogPublicationManifest recovered=p.repository.lookup(key);
                if(recovered!=null) {
                    try {
                        if(!usable(recovered,p.expiryMarginSeconds)) {
                            CatalogPublicationManifest renewed=p.repository.renew(recovered);
                            if(renewed!=null) { validateRenewal(recovered,renewed);recovered=renewed; }
                        }
                        read(p,key,id,recovered); manifest=recovered;
                    } catch(Exception invalid) { p.repository.invalidate(key,recovered.manifest_id()); }
                }
            }
            if(manifest!=null) {
                if(usable(manifest,p.expiryMarginSeconds)) {
                    if(!id.equals(p.identity.apply(request)))continue;
                    remember(p,key,manifest);
                    if(manifest.etag()!=null && manifest.etag().equals(inm)) return new Delivery(CatalogContents.unchanged(manifest.catalog_version(),manifest.etag()),null);
                    return new Delivery(null,manifest.reference());
                }
                CatalogPublicationManifest renewed=p.repository.renew(manifest);
                if(renewed!=null) { validateRenewal(manifest,renewed);read(p,key,id,renewed); remember(p,key,renewed); continue; }
                p.repository.invalidate(key,manifest.manifest_id()); validated.remove(key);
            }
            CatalogContentsResponse response=build.get();
            if(response.not_modified()) throw new IllegalArgumentException("publication build must return a full catalog");
            CatalogContents.schemas(response);
            if(response.catalog_version()!=id.catalog_version() || !id.equals(p.identity.apply(request))) continue;
            if(response.etag()==null) response=withEtag(response,digest(response));
            if(response.etag().equals(inm)) return new Delivery(CatalogContents.unchanged(response.catalog_version(),response.etag()),null);
            byte[] inner=RecordCodec.serializeToBytes(response);
            long[] sizes={0,0};
            ExternalStorage bounded=(body,encoding)->{
                if(body.length>p.maxStoredBytes) throw new IllegalArgumentException("catalog stored-size limit exceeded");
                long raw=body.length;
                if(encoding!=null) {
                    if(!encoding.equals("zstd")) throw new IllegalArgumentException("unsupported publication encoding");
                    try(InputStream decoded=new com.github.luben.zstd.ZstdInputStream(new ByteArrayInputStream(body))) { raw=0;byte[] buffer=new byte[8192];int count;while((count=decoded.read(buffer))>=0){raw+=count;if(raw>p.maxRawBytes)break;} }
                }
                if(raw>p.maxRawBytes) throw new IllegalArgumentException("catalog raw-size limit exceeded");
                sizes[0]=raw;sizes[1]=body.length;
                return p.repository.storage().upload(body,encoding);
            };
            ExternalRef reference;
            try(Marshalling.EncodedRow envelope=Marshalling.encodeRowForWire(OUTER,Map.of("result",inner),Allocators.root())) {
                reference=Externalizer.publishExternal(envelope.root(),bounded,p.external.compression(),true);
            }
            PublicationLocation location=p.repository.location(reference);
            manifest=new CatalogPublicationManifest(key,UUID.randomUUID().toString().replace("-",""),"vgi.v2",WIRE,contractHash(),CODEC,id.catalog_version(),response.etag(),reference.sha256(),sizes[0],sizes[1],reference.url(),location.object_id(),location.expires_at(),location.retain_until(),location.stable(),1,1);
            CatalogContentsResponse readback=read(p,key,id,manifest);
            if(!same(response,readback)) throw new IllegalStateException("published catalog differs from collected snapshot");
            if(!id.equals(p.identity.apply(request))) continue;
            CatalogPublicationManifest winner=p.repository.commitIfAbsent(manifest);
            if(!winner.manifest_id().equals(manifest.manifest_id())) read(p,key,id,winner);
            if(!id.equals(p.identity.apply(request)))continue;
            remember(p,key,winner);
            return new Delivery(null,winner.reference());
            }
        }
        throw new IllegalStateException("catalog revision or publication changed repeatedly");
    }

    public Delivery warm(CatalogSnapshotPolicy p, SnapshotRequest request, Supplier<CatalogContentsResponse> build) { return serve(p,request,null,build); }
    public void importPublication(CatalogSnapshotPolicy p, SnapshotRequest request, CatalogPublicationManifest manifest) throws Exception {
        SnapshotIdentity id=p.identity.apply(request);String key=key(p,request,id);
        try(var lease=lock(key)) {
        read(p,key,id,manifest);
        if(!id.equals(p.identity.apply(request)))throw new IllegalStateException("catalog changed during publication import");
        CatalogPublicationManifest winner=p.repository.commitIfAbsent(manifest);
        if(!winner.manifest_id().equals(manifest.manifest_id())) read(p,key,id,winner);
        if(!id.equals(p.identity.apply(request)))throw new IllegalStateException("catalog changed during publication import");
        remember(p,key,winner);
        }
    }
    public boolean invalidate(CatalogSnapshotPolicy p,String key,String manifestId) throws Exception { try(var lease=lock(key)){var old=validated.get(key);if(old!=null&&old.manifest_id().equals(manifestId))validated.remove(key);return p.repository.invalidate(key,manifestId);} }
    private synchronized void remember(CatalogSnapshotPolicy p,String key,CatalogPublicationManifest value) { if(validated.size()>=Math.max(1,p.maxCachedManifests)) validated.clear();validated.put(key,value); }

    private CatalogContentsResponse read(CatalogSnapshotPolicy p,String key,SnapshotIdentity id,CatalogPublicationManifest m) throws Exception {
        if(!m.cache_key().equals(key)||m.catalog_version()!=id.catalog_version()||!m.protocol_name().equals("vgi.v2")||!m.wire_revision().equals(WIRE)||!m.contract_hash().equals(contractHash())||!m.codec_revision().equals(CODEC)||m.format_version()!=1||m.validator_revision()!=1||(m.sha256()==null||m.sha256().length()!=64)||m.raw_size()<=0||m.raw_size()>p.maxRawBytes||m.stored_size()<=0||m.stored_size()>p.maxStoredBytes||!usable(m,p.expiryMarginSeconds)) throw new IllegalArgumentException("invalid or expired catalog publication manifest");
        ExternalLocationConfig config=ExternalLocationConfig.builder().maxFetchBytes(Math.min(p.external.maxFetchBytes(),p.maxStoredBytes)).maxDecompressedBytes(Math.min(p.external.maxDecompressedBytes(),p.maxRawBytes)).urlValidator(p.external.urlValidator()).maxRetries(p.external.maxRetries()).httpTimeout(p.external.httpTimeout()).build();
        byte[] bytes=new ExternalFetcher(config).fetch(URI.create(m.url()),m.sha256());
        if(bytes.length!=m.raw_size()) throw new IllegalArgumentException("catalog publication size mismatch");
        ByteArrayInputStream outerInput=new ByteArrayInputStream(bytes);
        try(IpcStreamReader reader=new IpcStreamReader(outerInput,Allocators.root())) {
            if(!reader.wireSchema().equals(OUTER)||!ordinaryBatch(reader)||reader.root().getRowCount()!=1) throw new IllegalArgumentException("invalid catalog RPC envelope");
            VarBinaryVector result=(VarBinaryVector)reader.root().getVector(0);
            if(result.isNull(0)) throw new IllegalArgumentException("catalog RPC result is null");
            byte[] inner=result.get(0);
            if(reader.readNextBatch()!=null||outerInput.available()!=0) throw new IllegalArgumentException("catalog RPC envelope has extra data");
            ByteArrayInputStream innerInput=new ByteArrayInputStream(inner);
            try(IpcStreamReader catalog=new IpcStreamReader(innerInput,Allocators.root())) {
                if(!catalog.wireSchema().equals(SchemaDerivation.schemaForRecord(CatalogContentsResponse.class))||!ordinaryBatch(catalog)||catalog.root().getRowCount()!=1) throw new IllegalArgumentException("invalid catalog response envelope");
                CatalogContentsResponse response=RecordCodec.fromRowMap(CatalogContentsResponse.class,Marshalling.decodeRow(catalog.root(),catalog.dictionaryProvider(),catalog.wireSchema()));
                if(catalog.readNextBatch()!=null||innerInput.available()!=0||response.not_modified()||response.catalog_version()!=m.catalog_version()||!Objects.equals(response.etag(),m.etag())) throw new IllegalArgumentException("catalog publication metadata mismatch");
                CatalogContents.schemas(response);
                return response;
            }
        }
    }
    private static boolean ordinaryBatch(IpcStreamReader reader) throws IOException {
        var metadata=reader.readNextBatch();return metadata!=null&&metadata.keySet().stream().noneMatch(key->key.startsWith("vgi_rpc.location")||key.startsWith("vgi_rpc.log_")||key.startsWith("vgi_rpc.error_"));
    }
    private static boolean usable(CatalogPublicationManifest m,double margin) { double deadline=System.currentTimeMillis()/1000.0+margin;return (m.stable()||m.expires_at()!=null)&&(m.expires_at()==null||(Double.isFinite(m.expires_at())&&m.expires_at()>deadline))&&(m.retain_until()==null||(Double.isFinite(m.retain_until())&&m.retain_until()>deadline)); }
    public static String hash(byte[] value) { try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); } catch(Exception e) { throw new IllegalStateException(e); } }
    private static String contractHash() { return farm.query.vgi.VgiService.CATALOG_CONTRACT_HASH; }
    static String key(CatalogSnapshotPolicy p,SnapshotRequest r,SnapshotIdentity id) {
        if(id.catalog_namespace()==null||id.catalog_namespace().isEmpty()||id.source_revision()==null||id.source_revision().isEmpty()||id.catalog_version()!=r.catalog_version()) throw new IllegalArgumentException("publication needs namespace, stable revision and matching catalog version");
        return hash(JSON.toJson(List.of("vgi.v2",WIRE,contractHash(),CODEC,id.catalog_namespace(),id.source_revision(),id.visibility_scope()==null?r.attachment_scope():id.visibility_scope(),id.catalog_version(),p.repository.profile(),String.valueOf(p.external.compression()))).getBytes(StandardCharsets.UTF_8));
    }
    private static byte[][] streams(CatalogContentsResponse r) { return new byte[][]{r.schemas(),r.tables(),r.views(),r.functions(),r.macros(),r.indexes()}; }
    public static String digest(CatalogContentsResponse r) {
        try {
            MessageDigest digest=MessageDigest.getInstance("SHA-256");digest.update("vgi.catalog.contents.2.2\0".getBytes(StandardCharsets.UTF_8));
            digest.update(java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(r.catalog_version()).array());
            for(byte[] stream:streams(r)){digest.update((byte)(stream==null?0:1));if(stream!=null){digest.update(java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(stream.length).array());digest.update(stream);}}
            return HexFormat.of().formatHex(digest.digest());
        } catch(Exception error){throw new IllegalStateException(error);}
    }
    private static void validateRenewal(CatalogPublicationManifest old,CatalogPublicationManifest renewed) {
        if(!old.manifest_id().equals(renewed.manifest_id()))return;
        var a=JSON.toJsonTree(old).getAsJsonObject();var b=JSON.toJsonTree(renewed).getAsJsonObject();
        a.remove("url");a.remove("expires_at");b.remove("url");b.remove("expires_at");
        if(!a.equals(b))throw new IllegalArgumentException("catalog renewal changed immutable publication metadata");
    }
    private static CatalogContentsResponse withEtag(CatalogContentsResponse r,String etag) { return new CatalogContentsResponse(r.catalog_version(),etag,r.not_modified(),r.schemas(),r.tables(),r.views(),r.functions(),r.macros(),r.indexes()); }
    private static boolean same(CatalogContentsResponse a,CatalogContentsResponse b) { return a.catalog_version()==b.catalog_version()&&Objects.equals(a.etag(),b.etag())&&Arrays.deepEquals(streams(a),streams(b)); }
}
