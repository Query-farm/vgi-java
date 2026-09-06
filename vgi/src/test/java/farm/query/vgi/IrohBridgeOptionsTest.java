// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class IrohBridgeOptionsTest {
    @Test
    void defaultsToAuthenticatedLoopbackBridge() {
        var options = new IrohBridgeOptions("test-mesh");
        assertEquals(Set.of("127.0.0.1"), options.trustedProxyAddresses());
        assertTrue(options.authenticate());
    }

    @Test
    void rejectsMissingIssuerAndNonLoopbackWorkerBinds() {
        assertThrows(IllegalArgumentException.class, () -> new IrohBridgeOptions(" "));
        var worker = Worker.builder();
        assertThrows(IllegalArgumentException.class,
                () -> worker.runIrohTcpUpstream(
                        "0.0.0.0", 9400, 0, new IrohBridgeOptions("test-mesh")));
        assertThrows(IllegalArgumentException.class,
                () -> worker.runHttp(
                        "0.0.0.0", 9401, new IrohBridgeOptions("test-mesh")));
    }
}
