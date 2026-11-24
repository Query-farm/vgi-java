// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi;

import farm.query.vgirpc.RpcServer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What a VGI worker advertises (PROTOCOL_VERSIONS_V1), and the VGI_TEST_PROTOCOL_RANGE switch. */
class WorkerProtocolVersionsTest {

    @Test
    void everyTransportAdvertisesVgiV3() {
        Worker w = Worker.builder().catalogName("pv_probe");
        for (Worker.Transport t : Worker.Transport.values()) {
            RpcServer server = w.buildServer(t);
            assertEquals("vgi.v3=3.4-3.4", server.protocolVersions(), t.name());
        }
    }

    @Test
    void theFixtureSwitchParsesBareAndNamedRanges() {
        assertEquals(Map.of(), Worker.testProtocolRanges("vgi.v3", null));
        assertEquals(Map.of(), Worker.testProtocolRanges("vgi.v3", "  "));
        assertEquals(Map.of("vgi.v3", "3.3-3.3"), Worker.testProtocolRanges("vgi.v3", "3.3-3.3"));
        assertEquals(Map.of("vgi.v3", "3.3-3.4"), Worker.testProtocolRanges("vgi.v3", "vgi.v3=3.3-3.4"));
        assertThrows(IllegalArgumentException.class, () -> Worker.testProtocolRanges("vgi.v3", "three"));
        assertThrows(IllegalArgumentException.class, () -> Worker.testProtocolRanges("vgi.v3", "vgi.v3=3.3, junk"));
    }

    @Test
    void aWindowOverrideMovesTheGate() {
        RpcServer server = Worker.builder().catalogName("pv_probe").buildServer(Worker.Transport.PIPE);
        server.setProtocolVersionRanges(Worker.testProtocolRanges("vgi.v3", "3.3-3.3"));
        assertEquals("vgi.v3=3.3-3.3", server.protocolVersions());
        assertNull(server.gateVersion("vgi.v3", "3.3.0"));
        String refusal = server.gateVersion("vgi.v3", "3.4.0").getMessage();
        assertTrue(refusal.contains("this server supports vgi.v3 3.3-3.3"), refusal);
        assertTrue(refusal.contains("The server is too old"), refusal);
    }
}
