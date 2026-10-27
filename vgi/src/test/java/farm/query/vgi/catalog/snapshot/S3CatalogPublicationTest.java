package farm.query.vgi.catalog.snapshot;
import farm.query.vgirpc.external.ExternalRef;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class S3CatalogPublicationTest {
    static class Store implements S3CatalogPublicationRepository.Client {
        final Map<String,S3CatalogPublicationRepository.VersionedObject> entries=new HashMap<>();int version,signs;
        public synchronized S3CatalogPublicationRepository.VersionedObject get(String b,String key,long max){var value=entries.get(key);if(value!=null&&value.bytes().length>max)throw new IllegalArgumentException("size");return value;}
        public synchronized boolean put(String b,String key,byte[] bytes,String type,String encoding,S3CatalogPublicationRepository.Condition condition){var old=entries.get(key);if(condition.matchingEtag()==null?old!=null:old==null||!condition.matchingEtag().equals(old.etag()))return false;entries.put(key,new S3CatalogPublicationRepository.VersionedObject(bytes.clone(),Integer.toString(++version)));return true;}
        public String presignGet(String b,String key,long ttl){return "https://objects.example/"+key+"?signature="+(++signs);}
    }
    @Test void durableDiscoveryConditionalInvalidationAndRenewal() throws Exception {
        var store=new Store();var first=new S3CatalogPublicationRepository(store,"bucket","catalog","profile",600,3600);String url=first.upload(new byte[]{1,2,3},null).toString();var location=first.location(new ExternalRef(url,"a".repeat(64)));
        var m=new CatalogPublicationManifest("key","manifest","vgi.v3","3.0",farm.query.vgi.VgiService.CATALOG_CONTRACT_HASH,"catalog-collections-1",1,"etag","a".repeat(64),3,3,url,location.object_id(),location.expires_at(),location.retain_until(),false,1,1);
        assertEquals(m,first.commitIfAbsent(m));var restarted=new S3CatalogPublicationRepository(store,"bucket","catalog","profile",600,3600);assertEquals(m,restarted.lookup("key"));
        var renewed=restarted.renew(m);assertNotEquals(m.url(),renewed.url());assertEquals(m.sha256(),renewed.sha256());assertEquals(renewed,first.lookup("key"));
        assertFalse(first.invalidate("key","wrong"));assertTrue(first.invalidate("key","manifest"));assertNull(restarted.lookup("key"));
        assertTrue(store.entries.containsKey(location.object_id()),"invalidation must retain issued immutable objects");assertEquals(m,restarted.commitIfAbsent(m));
    }
}
