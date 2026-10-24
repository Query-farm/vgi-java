package farm.query.vgi;

import com.sun.net.httpserver.HttpServer;
import farm.query.vgi.catalog.snapshot.*;
import farm.query.vgi.protocol.*;
import farm.query.vgirpc.external.*;
import org.junit.jupiter.api.Test;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class CatalogPublicationHttpTest {
    static class Objects implements AutoCloseable {
        final HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        final Map<String,byte[]> bytes=new ConcurrentHashMap<>();
        final AtomicInteger gets=new AtomicInteger(),uploads=new AtomicInteger();
        Objects() throws Exception {server.createContext("/",e->{gets.incrementAndGet();byte[] b=bytes.get(e.getRequestURI().getPath());e.sendResponseHeaders(b==null?404:200,b==null?0:b.length);if(b!=null)e.getResponseBody().write(b);e.close();});server.start();}
        ExternalStorage storage(){return (b,encoding)->{String path="/"+uploads.incrementAndGet();bytes.put(path,b);return URI.create("http://127.0.0.1:"+server.getAddress().getPort()+path);};}
        public void close(){server.stop(0);}
    }
    static CatalogSnapshotPolicy policy(CatalogPublicationRepository repo){return new CatalogSnapshotPolicy(repo,r->new SnapshotIdentity("catalog","r1",null,r.catalog_version()),ExternalLocationConfig.builder().urlValidator(uri->{}).maxRetries(0).build());}
    @Test void httpWorkerAuthenticatesBeforeReusingPublication() throws Exception {
        try(var objects=new Objects()){
            var repo=new MemoryCatalogPublicationRepository(objects.storage(),"test",new PublicationLocation(null,null,null,true));var p=policy(repo);var builds=new AtomicInteger();
            var worker=farm.query.vgi.Worker.builder().catalogName("published").opaqueDataKey(new byte[32]).catalogSnapshot(p).catalogContents(request->{builds.incrementAndGet();return farm.query.vgi.CatalogContentsResult.of(request.build(),"e1");});
            var server=new farm.query.vgirpc.http.HttpServer(worker.buildServer(farm.query.vgi.Worker.Transport.HTTP),farm.query.vgirpc.http.HttpServer.Config.builder().host("127.0.0.1").port(0).authenticator(request->new farm.query.vgirpc.AuthContext("test",true,request.getHeader("X-Principal"),Map.of())).build());
            server.start();
            try(var alice=farm.query.vgirpc.http.HttpRpcConnection.builder("http://127.0.0.1:"+server.port()).header("X-Principal","alice").externalLocation(p.external).build();var bob=farm.query.vgirpc.http.HttpRpcConnection.builder("http://127.0.0.1:"+server.port()).header("X-Principal","bob").externalLocation(p.external).build()){
                var api=alice.proxy(farm.query.vgi.VgiService.class);var attach=api.catalog_attach(CatalogAttachRequest.of("published",null,null,null),null).attach_opaque_data();
                assertFalse(api.catalog_contents(attach,null,null).not_modified());assertEquals(1,objects.uploads.get());assertEquals(2,objects.gets.get());
                assertFalse(api.catalog_contents(attach,null,null).not_modified());assertEquals(1,builds.get());assertEquals(3,objects.gets.get());
                assertTrue(api.catalog_contents(attach,"e1",null).not_modified());assertEquals(3,objects.gets.get());
                assertThrows(farm.query.vgirpc.RpcError.class,()->bob.proxy(farm.query.vgi.VgiService.class).catalog_contents(attach,null,null));assertEquals(1,objects.uploads.get());
                var second=api.catalog_attach(CatalogAttachRequest.of("published",null,null,null),null).attach_opaque_data();api.catalog_contents(second,null,null);assertEquals(2,objects.uploads.get());
            } finally {server.stop();}
        }
    }

}
