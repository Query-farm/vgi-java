// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.PartitionKind;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code cache_partitioned(rows_per_country)} — cacheable single-value-partitioned
 * result that emits {@code partition_values} on every batch.
 *
 * <p>No other cacheable fixture emits partition values, so the non-empty
 * {@code pv_bytes} framing in the disk blob (the capture writes {@code pv_len+pv};
 * the streaming load reads {@code pv_len} then seeks past it) would otherwise be
 * untested. A misframed {@code pv_len} would misalign the streaming
 * table-of-contents seek and the {@code GROUP BY} would return wrong rows.
 *
 * <p>Single worker, so the five-batch output is deterministic.
 */
public final class CachePartitionedFunction extends SimpleTableFunction {

    private static final String[] COUNTRIES = {"AU", "BR", "CA", "FR", "US"};

    private static final Schema OUTPUT = Schemas.of(
            EmitMetadata.partitionField("country", Schemas.UTF8),
            Schemas.nullable("sales", Schemas.INT64));

    private static final FunctionSpec SPEC = FunctionSpec.builder("cache_partitioned")
            .metadata(FunctionMetadata.describe(
                    "Cacheable single-value-partitioned result (partition_values through the spill blob)")
                    .withCategories("generator", "cache", "testing", "partitioning")
                    .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
            .constArg("rows_per_country", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override protected Schema outputSchema() { return OUTPUT; }

    @Override public TableProducerState createProducer(TableInitParams p) {
        return new State((int) ParameterExtractor.of(p.arguments())
                .positional(0, "rows_per_country").asLong().required());
    }

    /** Cursor over the fixed country list; one batch per country. */
    public static final class State extends TableProducerState {
        /** Index of the next country to emit. */
        public int countryIdx;
        /** Rows emitted per country. */
        public int rowsPerCountry;
        /** Whether the cache control has been advertised. */
        public boolean advertised;

        /** Required no-arg constructor for state deserialization. */
        public State() {}

        State(int rowsPerCountry) { this.rowsPerCountry = rowsPerCountry; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (countryIdx >= COUNTRIES.length) { out.finish(); return; }
            String country = COUNTRIES[countryIdx];
            long base = (long) countryIdx * 1_000_000L;
            countryIdx++;

            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
            boolean emitted = false;
            try {
                root.allocateNew();
                VarCharVector cv = (VarCharVector) root.getVector("country");
                BigIntVector sv = (BigIntVector) root.getVector("sales");
                Text countryText = new Text(country);
                for (int i = 0; i < rowsPerCountry; i++) {
                    cv.setSafe(i, countryText);
                    sv.setSafe(i, base + i);
                }
                root.setRowCount(rowsPerCountry);

                Map<String, String> md = new LinkedHashMap<>(
                        EmitMetadata.partitionValues(OUTPUT, root, null));
                if (!advertised) {
                    advertised = true;
                    md.putAll(CacheControl.ttl(CacheFunctions.DEFAULT_TTL_SECONDS).toMetadata());
                }
                out.emit(root, md);
                emitted = true;
            } finally {
                if (!emitted) root.close();
            }
        }
    }
}
