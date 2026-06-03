// Copyright 2026 Query Farm LLC - https://query.farm

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

    /** Creates an empty cursor for Jackson round-tripping. */
    public BatchState() {}

    /**
     * Creates a cursor over {@code total} rows emitted in {@code batchSize} chunks.
     *
     * @param total total number of rows to emit
     * @param batchSize rows per batch; floored at 1
     */
    public BatchState(long total, long batchSize) {
        this.total = total;
        this.batchSize = Math.max(1, batchSize);
    }

    /** @return the total number of rows to emit */
    public long total() { return total; }

    /** @return the configured rows-per-batch */
    public long batchSize() { return batchSize; }

    /** @return the number of rows already emitted */
    public long index() { return index; }

    /** @return {@code true} once every row has been emitted */
    public boolean done() { return index >= total; }

    /** @return the row count for the next batch, clamped to the remaining rows */
    public int nextBatchSize() {
        long remaining = total - index;
        return (int) Math.min(batchSize, remaining);
    }

    /**
     * Advances the cursor after emitting a batch.
     *
     * @param n the number of rows just emitted
     */
    public void advance(long n) { index += n; }
}
