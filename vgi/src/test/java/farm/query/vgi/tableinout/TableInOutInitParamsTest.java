// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.storage.BoundStorage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * {@link TableInOutInitParams#substreamStateKey()}: the row a table-in-out substream's state is
 * stored under is the client's {@code substream_id}, and the process id only when it sent none.
 *
 * <p>The end-to-end proof -- two substreams of one execution, served by one process, both reaching
 * {@code finish()} -- runs the real example fixtures in {@code vgi-example-worker}'s
 * {@code SubstreamStateKeyTest}. This pins the choice itself where the framework's own suite runs.
 */
final class TableInOutInitParamsTest {

    private static TableInOutInitParams withSubstream(byte[] substreamId) {
        return new TableInOutInitParams("f", null, null, null, null, null, null, null, substreamId);
    }

    /** Keyed per substream: two connections one process serves must not share a row. */
    @Test
    void theKeyIsTheSubstreamIdTheClientSent() {
        byte[] id = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        byte[] key = withSubstream(id).substreamStateKey();
        assertArrayEquals(id, key);
        assertNotSame(id, key, "the key is a copy, so a caller mutating it cannot move the row");
    }

    /** Only a client that sent none is keyed by process -- the behaviour it had before. */
    @Test
    void aClientSendingNoSubstreamIdIsKeyedByProcess() {
        assertArrayEquals(BoundStorage.packIntKey(ProcessHandle.current().pid()),
                withSubstream(null).substreamStateKey());
        assertArrayEquals(BoundStorage.packIntKey(ProcessHandle.current().pid()),
                new TableInOutInitParams("f", null, null, null, null, null, null, null).substreamStateKey(),
                "the serial-path constructor carries no substream id");
    }
}
