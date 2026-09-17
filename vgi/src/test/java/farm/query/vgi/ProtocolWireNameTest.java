// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.RpcServer;
import farm.query.vgirpc.ServiceIntrospector;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The VGI protocol's wire name, pinned.
 *
 * <p>{@code vgi.v2} is a cross-port contract, not a local choice: the DuckDB C++ extension emits
 * it from a generated header, and every implementation must answer to the same string or a client
 * cannot address them all. Nothing else in this repo's suite would notice it changing —
 * a worker that renamed itself passes every functional test it has, because its own client
 * derives the same wrong name, and fails only against the extension, as a 404.
 *
 * <p>Two assertions, because they fail for different reasons. The constant pins the string a
 * reader can check against the other ports. Resolving it through {@link ServiceIntrospector}
 * proves the <em>declaration took effect</em> — that the annotation is present and read — which a
 * constant alone says nothing about: before {@code @ProtocolName} existed the wire name was this
 * interface's simple name, and a declaration that silently did not apply would leave the worker
 * answering to {@code VgiService} with this constant sitting right there claiming otherwise.
 */
final class ProtocolWireNameTest {

    @Test
    void theWireNameIsVgiV2() {
        assertEquals("vgi.v2", VgiService.PROTOCOL_NAME);
    }

    @Test
    void theDeclarationIsWhatTheTransportResolves() {
        assertEquals(VgiService.PROTOCOL_NAME, ServiceIntrospector.protocolName(VgiService.class),
                "the wire name must come from the @ProtocolName declaration, not from the Java "
                        + "interface's simple name");
    }

    @Test
    void aWorkerHostsIt() {
        // Through the object that actually answers requests: the name the server advertises and
        // routes on is what a client reaches, and it is resolved once at construction.
        RpcServer server = Worker.builder().catalogName("wire_name_probe").rpcServer();
        assertEquals("vgi.v2", server.protocolName());
    }

    /**
     * The secret protocol ({@code vgi.secret.v1}) is a separate service with its own major, and
     * this port does not host it — there is no {@code secret_lookup} surface here, only the
     * consumption of secrets at bind time. Asserted rather than left implicit so that adding the
     * service without giving it its own declaration is caught: inheriting or deriving a name is
     * how a co-hosted protocol ends up impersonating its neighbour.
     */
    @Test
    void thisPortDoesNotHostTheSecretProtocol() throws Exception {
        Path root = mainSourceRoot();
        List<Path> offenders;
        try (var files = Files.walk(root)) {
            offenders = files.filter(p -> p.toString().endsWith(".java"))
                    .filter(ProtocolWireNameTest::declaresSecretLookup)
                    .toList();
        }
        assertTrue(offenders.isEmpty(),
                () -> "a secret_lookup surface appeared; it is the 'vgi.secret.v1' protocol and "
                        + "needs its own @ProtocolName declaration, not an inherited or derived "
                        + "name: " + offenders);
    }

    private static boolean declaresSecretLookup(Path file) {
        try {
            return Files.readString(file).contains("secret_lookup(");
        } catch (Exception e) {
            return false;
        }
    }

    /** Locate {@code src/main/java} from wherever the test runner set the working directory. */
    private static Path mainSourceRoot() {
        Path dir = Path.of("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++, dir = dir.getParent()) {
            for (Path candidate : List.of(dir.resolve("src/main/java"),
                    dir.resolve("vgi/src/main/java"))) {
                if (Files.isDirectory(candidate)) return candidate;
            }
        }
        throw new IllegalStateException(
                "could not locate src/main/java from " + Path.of("").toAbsolutePath());
    }
}
