// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingStore;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.BatchUtil;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * {@code batch_index_buffer_input} — demands a {@code batch_index} per
 * {@code process()} ({@code requires_input_batch_index=true}); packs
 * {@code (batch_index, ipc)} into an unsorted log, sorts globally by index in
 * {@code combine()}, then drains in order. Mirrors vgi-python
 * {@code BatchIndexBufferInputFunction}.
 */
public final class BatchIndexBufferInputFunction extends AbstractBufferAndDrain {

    private static final byte[] NS_UNSORTED = "unsorted".getBytes(StandardCharsets.UTF_8);

    @Override public String name() { return "batch_index_buffer_input"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("buffer_input variant using batch_index to reconstruct order")
                .withCategories("test", "ordering");
    }

    @Override public boolean requiresInputBatchIndex() { return true; }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        if (params.batchIndex() == null) {
            throw new IllegalStateException(
                    "batch_index_buffer_input.process() received batch_index=null "
                    + "— requiresInputBatchIndex plumbing is broken");
        }
        byte[] ipc = BatchUtil.writeSingleBatch(batch);
        ByteBuffer packed = ByteBuffer.allocate(8 + ipc.length);
        packed.putLong(params.batchIndex());  // big-endian; order-preserving for sort
        packed.put(ipc);
        params.storage().stateAppend(NS_UNSORTED, KEY, packed.array());
        return params.executionId();
    }

    @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        record Pair(long idx, byte[] ipc) {}
        List<Pair> pairs = new ArrayList<>();
        for (BufferingStore.Entry e : params.storage().stateLogScan(NS_UNSORTED, KEY, -1, Integer.MAX_VALUE)) {
            ByteBuffer b = ByteBuffer.wrap(e.value());
            long idx = b.getLong();
            byte[] ipc = new byte[b.remaining()];
            b.get(ipc);
            pairs.add(new Pair(idx, ipc));
        }
        pairs.sort(Comparator.comparingLong(Pair::idx));
        for (Pair p : pairs) {
            params.storage().stateAppend(NS_BUF, KEY, p.ipc());
        }
        return List.of(params.executionId());
    }
}
