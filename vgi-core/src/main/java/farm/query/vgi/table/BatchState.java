// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

/**
 * Iteration cursor for batch-emitting producers. Tracks total row count and
 * emits batches of {@code batchSize}; mirrors vgi-go's {@code BatchState}.
 *
 * <p>Field shape is a plain mutable POJO with a public no-arg constructor so
 * Jackson can round-trip it as a nested component of a {@link
 * farm.query.vgirpc.StreamState}.</p>
 *
 * <pre>{@code
 * BatchState bs = new BatchState(count, batchSize);
 * if (bs.done()) { out.finish(); return; }
 * int n = bs.nextBatchSize();
 * out.emit(...);
 * bs.advance(n);
 * }</pre>
 */
public final class BatchState {

    private long total;
    private long batchSize;
    private long index;

    public BatchState() {}

    public BatchState(long total, long batchSize) {
        this.total = total;
        this.batchSize = Math.max(1, batchSize);
    }

    public long getTotal() { return total; }
    public void setTotal(long v) { this.total = v; }

    public long getBatchSize() { return batchSize; }
    public void setBatchSize(long v) { this.batchSize = v; }

    public long getIndex() { return index; }
    public void setIndex(long v) { this.index = v; }

    public long total() { return total; }
    public long batchSize() { return batchSize; }
    public long index() { return index; }

    public boolean done() { return index >= total; }

    public int nextBatchSize() {
        long remaining = total - index;
        return (int) Math.min(batchSize, remaining);
    }

    public void advance(long n) { index += n; }
}
