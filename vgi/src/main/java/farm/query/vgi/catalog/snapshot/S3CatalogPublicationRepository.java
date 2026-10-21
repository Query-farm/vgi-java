// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.catalog.snapshot;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import farm.query.vgirpc.external.ExternalRef;
import farm.query.vgirpc.external.ExternalStorage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** S3/R2 durable discovery and immutable objects; the injected SDK client owns credentials. */
public final class S3CatalogPublicationRepository implements CatalogPublicationRepository, ExternalStorage {
    public record VersionedObject(byte[] bytes, String etag) {}
    /** null matchingEtag means If-None-Match: *. Otherwise use If-Match exactly. */
    public record Condition(String matchingEtag) {}
    public interface Client {
        /** Abort before buffering more than maxBytes; null means not found only. */
        VersionedObject get(String bucket, String key, long maxBytes) throws Exception;
        /** false means precondition failed only; propagate transport/authorization failures. */
        boolean put(String bucket, String key, byte[] bytes, String contentType, String contentEncoding, Condition condition) throws Exception;
        String presignGet(String bucket, String key, long lifetimeSeconds) throws Exception;
    }
    private record Loaded(CatalogPublicationManifest manifest, String etag) {}
    private static final Gson JSON = new Gson();
    private final Client client;
    private final String bucket, prefix, profile;
    private final long urlTtl, retention;
    private final ConcurrentHashMap<String, PublicationLocation> locations = new ConcurrentHashMap<>();
    public S3CatalogPublicationRepository(Client client, String bucket, String prefix, String profile, long urlTtlSeconds, long retentionSeconds) {
        if(bucket.isBlank()||profile.isBlank()||urlTtlSeconds<=0||retentionSeconds<=urlTtlSeconds) throw new IllegalArgumentException("bucket/profile and retention exceeding URL lifetime required");
        this.client=client;this.bucket=bucket;this.prefix=prefix.replaceAll("/+$", "")+"/";this.profile=profile;urlTtl=urlTtlSeconds;retention=retentionSeconds;
    }
    public String profile(){return profile;}
    public ExternalStorage storage(){return this;}
    private String key(String value){return prefix+"manifests/"+value+".json";}
    private Loaded load(String key) throws Exception {
        var object=client.get(bucket,key(key),1024*1024);
        if(object==null)return new Loaded(null,null);
        if(object.bytes().length>1024*1024)throw new IllegalArgumentException("catalog manifest exceeds limit");
        var json=JsonParser.parseString(new String(object.bytes(),StandardCharsets.UTF_8)).getAsJsonObject();
        return new Loaded(json.has("tombstone")&&json.get("tombstone").getAsBoolean()?null:JSON.fromJson(json,CatalogPublicationManifest.class),object.etag());
    }
    private boolean put(CatalogPublicationManifest m,String etag)throws Exception{return client.put(bucket,key(m.cache_key()),JSON.toJson(m).getBytes(StandardCharsets.UTF_8),"application/json",null,new Condition(etag));}
    public CatalogPublicationManifest lookup(String key)throws Exception{return load(key).manifest();}
    public CatalogPublicationManifest commitIfAbsent(CatalogPublicationManifest m)throws Exception{
        for(int i=0;i<8;i++){var old=load(m.cache_key());if(old.manifest()!=null)return old.manifest();if(put(m,old.etag()))return m;}
        throw new IllegalStateException("catalog manifest contention");
    }
    public boolean invalidate(String key,String manifestId)throws Exception{
        var old=load(key);if(old.manifest()==null||!old.manifest().manifest_id().equals(manifestId))return false;
        return client.put(bucket,key(key),"{\"tombstone\":true}".getBytes(StandardCharsets.UTF_8),"application/json",null,new Condition(old.etag()));
    }
    public URI upload(byte[] body,String contentEncoding)throws Exception{
        String object=prefix+"objects/"+UUID.randomUUID()+".arrow";
        if(!client.put(bucket,object,body,"application/vnd.apache.arrow.stream",contentEncoding,new Condition(null)))throw new IllegalStateException("immutable object collision");
        String url=client.presignGet(bucket,object,urlTtl);double now=System.currentTimeMillis()/1000.0;
        locations.put(url,new PublicationLocation(object,now+urlTtl,now+retention,false));return URI.create(url);
    }
    public PublicationLocation location(ExternalRef ref){var value=locations.remove(ref.url().toString());if(value==null)throw new IllegalArgumentException("reference was not uploaded by this repository");return value;}
    public CatalogPublicationManifest renew(CatalogPublicationManifest manifest)throws Exception{
        for(int i=0;i<8;i++){
            var old=load(manifest.cache_key());var m=old.manifest();if(m==null)return null;if(!m.manifest_id().equals(manifest.manifest_id()))return m;
            double now=System.currentTimeMillis()/1000.0;if(m.object_id()==null||m.retain_until()==null||m.retain_until()-now<=60)return null;
            long lifetime=Math.min(urlTtl,(long)(m.retain_until()-now));
            var renewed=new CatalogPublicationManifest(m.cache_key(),m.manifest_id(),m.protocol_name(),m.wire_revision(),m.contract_hash(),m.codec_revision(),m.catalog_version(),m.etag(),m.sha256(),m.raw_size(),m.stored_size(),client.presignGet(bucket,m.object_id(),lifetime),m.object_id(),now+lifetime,m.retain_until(),false,m.format_version(),m.validator_revision());
            if(put(renewed,old.etag()))return renewed;
        }
        throw new IllegalStateException("catalog manifest renewal contention");
    }
}
