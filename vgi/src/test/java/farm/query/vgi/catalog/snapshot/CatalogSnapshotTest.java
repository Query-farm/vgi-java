package farm.query.vgi.catalog.snapshot;
import com.sun.net.httpserver.HttpServer;
import farm.query.vgi.MetadataCollections;
import farm.query.vgi.protocol.*;
import farm.query.vgirpc.external.*;
import org.junit.jupiter.api.Test;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class CatalogSnapshotTest {
    static class Objects implements AutoCloseable {
        final HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        final Map<String,byte[]> bytes=new ConcurrentHashMap<>();final AtomicInteger gets=new AtomicInteger(),uploads=new AtomicInteger();
        Objects() throws Exception {server.createContext("/",e->{gets.incrementAndGet();byte[] b=bytes.get(e.getRequestURI().getPath());e.sendResponseHeaders(b==null?404:200,b==null?0:b.length);if(b!=null)e.getResponseBody().write(b);e.close();});server.start();}
        ExternalStorage storage(){return (b,encoding)->{String path="/"+uploads.incrementAndGet();bytes.put(path,b);return URI.create("http://127.0.0.1:"+server.getAddress().getPort()+path);};}
        public void close(){server.stop(0);}
    }
    static CatalogContentsResponse response(){return new CatalogContentsResponse(1,"e1",false,MetadataCollections.encode(List.of(new SchemaInfo(null,Map.of(),List.of("main"),null),new SchemaInfo(null,Map.of(),List.of("extra"),null)),SchemaInfo.class),null,null,MetadataCollections.encode(farm.query.vgi.internal.FunctionBinaryDictionaryTest.rows(),FunctionInfo.class,3),null,null);}
    static CatalogSnapshotPolicy policy(CatalogPublicationRepository repo){return new CatalogSnapshotPolicy(repo,r->new SnapshotIdentity("catalog","r1",null,r.catalog_version()),ExternalLocationConfig.builder().urlValidator(uri->{}).maxRetries(0).build());}
    @Test void warmRecoveryScopeAndConditionalHits() throws Exception {
        try(var objects=new Objects()){
            var repo=new MemoryCatalogPublicationRepository(objects.storage(),"test",new PublicationLocation(null,null,null,true));var policy=policy(repo);var req=new SnapshotRequest(new byte[]{1},"sealed-a",1,null);var builds=new AtomicInteger();
            java.util.function.Supplier<CatalogContentsResponse> build=()->{builds.incrementAndGet();return response();};
            assertNotNull(policy.manager.warm(policy,req,build).reference());assertEquals(1,objects.gets.get());
            assertTrue(policy.manager.serve(policy,req,"e1",build).response().not_modified());policy.manager.warm(policy,req,build);assertEquals(1,builds.get());assertEquals(1,objects.gets.get());
            var restarted=policy(repo);restarted.manager.warm(restarted,req,()->{throw new AssertionError("recovered publication rebuilt");});assertEquals(2,objects.gets.get());assertEquals(1,objects.uploads.get());
            restarted.manager.warm(restarted,new SnapshotRequest(new byte[]{1},"sealed-b",1,null),build);assertEquals(2,objects.uploads.get());
        }
    }
    @Test void publicationUsesV3AndRefusesPublishedV2Manifests() throws Exception {
        try(var objects=new Objects()){
            var repo=new MemoryCatalogPublicationRepository(objects.storage(),"test",new PublicationLocation(null,null,null,true));var p=policy(repo);var req=new SnapshotRequest(new byte[]{1},"v3",1,null);
            p.manager.warm(p,req,CatalogSnapshotTest::response);var m=repo.lookup(CatalogSnapshotManager.key(p,req,p.identity.apply(req)));
            assertEquals("vgi.v3",m.protocol_name());assertEquals("3.0",m.wire_revision());
            var previous=new CatalogPublicationManifest(m.cache_key(),m.manifest_id(),"vgi.v2","2.2",m.contract_hash(),m.codec_revision(),m.catalog_version(),m.etag(),m.sha256(),m.raw_size(),m.stored_size(),m.url(),m.object_id(),m.expires_at(),m.retain_until(),m.stable(),m.format_version(),m.validator_revision());
            assertThrows(IllegalArgumentException.class,()->p.manager.importPublication(p,req,previous));assertEquals(1,objects.gets.get());
        }
    }
    @Test void capsRejectBeforeUploadAndCorruptionIsNotCommitted() throws Exception {
        try(var objects=new Objects()){
            var repo=new MemoryCatalogPublicationRepository(objects.storage(),"test",new PublicationLocation(null,null,null,true));var policy=policy(repo);policy.maxRawBytes=1;var req=new SnapshotRequest(new byte[]{1},"a",1,null);
            assertThrows(IllegalStateException.class,()->policy.manager.warm(policy,req,CatalogSnapshotTest::response));assertEquals(0,objects.uploads.get());
            var corruptRepo=new MemoryCatalogPublicationRepository((b,e)->objects.storage().upload(new byte[]{0,1},e),"corrupt",new PublicationLocation(null,null,null,true));var corrupt=policy(corruptRepo);
            assertThrows(IllegalStateException.class,()->corrupt.manager.warm(corrupt,req,CatalogSnapshotTest::response));
        }
    }
    @Test void collectionsKeepCanonicalIndexesAcrossRowsBatchesAndSchemas() {
        var rows = new ArrayList<IndexInfo>();
        byte[] options = farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(new OptionState(new byte[0], List.of()));
        byte[] table = farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(new CatalogObjectRef("table", List.of("main"), "table", null));
        byte[] key = farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(new IndexKey(List.of("column"), null));
        for (int i = 0; i < 7; i++) {
            byte[] definition = farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(new IndexDefinition(
                    table, "ART", List.of(key), options, i % 2 == 0 ? "PRIMARY" : "UNIQUE", null));
            rows.add(new IndexInfo(i % 2 == 0 ? null : "comment", Map.of("tag", "v"), "i" + i,
                    List.of(i % 2 == 0 ? "main" : "other"), definition, options));
        }
        var decoded = MetadataCollections.decode(MetadataCollections.encode(rows, IndexInfo.class, 2), IndexInfo.class);
        assertEquals(rows.size(), decoded.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals(rows.get(i).name(), decoded.get(i).name());
            assertEquals(rows.get(i).schema_path(), decoded.get(i).schema_path());
            assertEquals(rows.get(i).comment(), decoded.get(i).comment());
            assertEquals(rows.get(i).tags(), decoded.get(i).tags());
            assertArrayEquals(rows.get(i).definition(), decoded.get(i).definition());
            assertArrayEquals(rows.get(i).options(), decoded.get(i).options());
        }
        assertNull(MetadataCollections.encode(List.of(), IndexInfo.class));
    }
    @Test void blockedKeyDoesNotBlockAnUnrelatedPublication() throws Exception {
        try(var objects=new Objects();var pool=Executors.newFixedThreadPool(2)){
            var repo=new MemoryCatalogPublicationRepository(objects.storage(),"test",new PublicationLocation(null,null,null,true));var p=policy(repo);var first=new SnapshotRequest(new byte[]{1},"blocked",1,null);
            int stripe=Math.floorMod(CatalogSnapshotManager.key(p,first,p.identity.apply(first)).hashCode(),64);
            SnapshotRequest selected=null;for(int i=0;i<1000;i++){var r=new SnapshotRequest(new byte[]{1},"other"+i,1,null);if(Math.floorMod(CatalogSnapshotManager.key(p,r,p.identity.apply(r)).hashCode(),64)!=stripe){selected=r;break;}}
            var other=selected;var started=new CountDownLatch(1);var release=new CountDownLatch(1);
            var blocked=pool.submit(()->p.manager.warm(p,first,()->{started.countDown();try{release.await();}catch(InterruptedException e){throw new RuntimeException(e);}return response();}));
            assertTrue(started.await(2,TimeUnit.SECONDS));
            try{assertNotNull(pool.submit(()->p.manager.warm(p,other,CatalogSnapshotTest::response)).get(2,TimeUnit.SECONDS).reference());}finally{release.countDown();}
            assertNotNull(blocked.get(2,TimeUnit.SECONDS).reference());
        }
    }

    @Test void importRejectsLogExtraAndTrailingDataWithMatchingHash() throws Exception {
        try(var objects=new Objects()){
            var repo=new MemoryCatalogPublicationRepository(objects.storage(),"test",new PublicationLocation(null,null,null,true));var p=policy(repo);var req=new SnapshotRequest(new byte[]{1},"strict",1,null);
            p.manager.warm(p,req,CatalogSnapshotTest::response);var m=repo.lookup(CatalogSnapshotManager.key(p,req,p.identity.apply(req)));
            var schema=new org.apache.arrow.vector.types.pojo.Schema(List.of(new org.apache.arrow.vector.types.pojo.Field("result",org.apache.arrow.vector.types.pojo.FieldType.notNullable(org.apache.arrow.vector.types.pojo.ArrowType.Binary.INSTANCE),List.of())));
            for(int mode=0;mode<3;mode++){
                var output=new java.io.ByteArrayOutputStream();
                try(var encoded=farm.query.vgirpc.marshal.Marshalling.encodeRowForWire(schema,Map.of("result",farm.query.vgirpc.marshal.RecordCodec.serializeToBytes(response())),farm.query.vgirpc.wire.Allocators.root());var writer=new farm.query.vgirpc.wire.IpcStreamWriter(output)){
                    writer.writeBatch(encoded.root(),mode==0?Map.of("vgi_rpc.log_level","INFO"):null);if(mode==1)writer.writeBatch(encoded.root(),null);
                }
                if(mode==2)output.write(1);byte[] bytes=output.toByteArray();objects.bytes.put(URI.create(m.url()).getPath(),bytes);
                var forged=new CatalogPublicationManifest(m.cache_key(),m.manifest_id(),m.protocol_name(),m.wire_revision(),m.contract_hash(),m.codec_revision(),m.catalog_version(),m.etag(),CatalogSnapshotManager.hash(bytes),bytes.length,bytes.length,m.url(),m.object_id(),m.expires_at(),m.retain_until(),m.stable(),m.format_version(),m.validator_revision());
                assertThrows(Exception.class,()->p.manager.importPublication(p,req,forged));
            }
        }
    }

}
