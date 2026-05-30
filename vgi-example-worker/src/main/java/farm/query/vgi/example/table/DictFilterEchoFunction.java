// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.CountdownTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@code dict_filter_echo(count [, batch_size])} — emits a dictionary-encoded
 * VARCHAR column to exercise filter pushdown over a dict column whose
 * DuckDB-facing type is plain VARCHAR.
 *
 * <p>The {@code s} column is declared as Arrow {@code dictionary<int8, utf8>}
 * <em>without</em> ENUM metadata, so DuckDB types it as VARCHAR and pushes a
 * VARCHAR (string) literal down for {@code WHERE s = 'x'} / {@code s IN (...)}.
 * The worker then emits {@code s} dictionary-encoded, producing a (dictionary
 * column, string literal) pair the auto-applied filter must compare. The
 * generic {@link PushdownFilters#evaluate} reads {@code FieldVector.getObject},
 * which on a dict-index vector returns the integer index rather than the
 * value — so we evaluate the filter against a <em>decoded</em> (plain UTF8)
 * copy of the row data, the "decode the column to its value type" path this
 * fixture exists to pin. Mirrors vgi-python's {@code DictFilterEchoFunction}.
 *
 * <p>Row {@code i} carries {@code s = ("red", "green", "blue")[i % 3]}.
 */
public final class DictFilterEchoFunction extends CountdownTableFunction {

    private static final List<String> DICT_VALUES = List.of("red", "green", "blue");

    // Dictionary id is scoped to the per-batch DictionaryProvider we ship
    // alongside the data, so any constant works — it only has to agree
    // between the schema field's encoding and the registered Dictionary.
    private static final long DICT_ID = 1L;
    private static final ArrowType.Int INDEX_TYPE = new ArrowType.Int(8, true);

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            dictField("s"));

    /** A {@code dictionary<int8, utf8>} field (no ENUM metadata → VARCHAR in DuckDB). */
    private static Field dictField(String name) {
        DictionaryEncoding enc = new DictionaryEncoding(DICT_ID, false, INDEX_TYPE);
        return new Field(name, new FieldType(true, Schemas.UTF8, enc), null);
    }

    @Override public String name() { return "dict_filter_echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Emits a dictionary-encoded VARCHAR column for filter-pushdown testing")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withCategories("generator", "diagnostic", "testing");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override protected long defaultBatchSize() { return 2048L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long count = p.positional(0, "count").asLong().required();
        long batchSize = p.named("batch_size").asLong().orElse(2048L);
        // Column names DuckDB asked for, in order (projection pushdown). The
        // framework already narrowed params.outputSchema() to the projected
        // subset; we only need the names — the dict shape is rebuilt locally.
        List<String> projected = new ArrayList<>();
        for (Field f : params.outputSchema().getFields()) projected.add(f.getName());
        return new State(new BatchState(count, batchSize), projected,
                params.pushdownFilters(), params.joinKeys());
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public List<String> projected;
        public byte[] filterBytes;
        public List<byte[]> joinKeysIpc;

        public State() {}

        State(BatchState batch, List<String> projected, byte[] filterBytes,
                List<byte[]> joinKeysIpc) {
            this.batch = batch;
            this.projected = projected;
            this.filterBytes = filterBytes;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) { out.finish(); return; }
            int n = batch.nextBatchSize();
            long start = batch.index();

            long[] nVals = new long[n];
            String[] sVals = new String[n];
            for (int i = 0; i < n; i++) {
                long row = start + i;
                nVals[i] = row;
                sVals[i] = DICT_VALUES.get((int) (row % DICT_VALUES.size()));
            }

            boolean[] mask = evaluateMask(nVals, sVals);
            int kept = 0;
            for (boolean b : mask) if (b) kept++;

            emitBatch(out, nVals, sVals, mask, kept);
            batch.advance(n);
        }

        /**
         * Evaluate the pushed filters against a plain-UTF8 copy of the row
         * data so string comparisons see the decoded values, not dict
         * indices. Returns an all-pass mask when no filter was pushed.
         */
        private boolean[] evaluateMask(long[] nVals, String[] sVals) {
            boolean[] all = new boolean[nVals.length];
            Arrays.fill(all, true);
            if (filterBytes == null) return all;
            PushdownFilters pf = PushdownFiltersDecoder.decode(
                    filterBytes, joinKeysIpc == null ? List.of() : joinKeysIpc);
            if (pf.filters().isEmpty()) return all;
            Schema plain = Schemas.of(
                    Schemas.nullable("n", Schemas.INT64),
                    Schemas.nullable("s", Schemas.UTF8));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(plain, Allocators.root())) {
                root.allocateNew();
                BigIntVector nv = (BigIntVector) root.getVector("n");
                VarCharVector sv = (VarCharVector) root.getVector("s");
                for (int i = 0; i < nVals.length; i++) {
                    nv.setSafe(i, nVals[i]);
                    sv.setSafe(i, new Text(sVals[i]));
                }
                root.setRowCount(nVals.length);
                return pf.evaluate(root);
            }
        }

        /**
         * Build and emit the projected output batch for the rows passing
         * {@code mask}, with {@code s} dictionary-encoded plus a matching
         * {@link DictionaryProvider}. The framework closes the emitted root
         * and the provider's dict vector after writing (see {@code RpcServer}).
         */
        private void emitBatch(OutputCollector out, long[] nVals, String[] sVals,
                boolean[] mask, int kept) {
            BufferAllocator alloc = Allocators.root();
            List<Field> fields = new ArrayList<>();
            List<FieldVector> vectors = new ArrayList<>();
            BigIntVector outN = null;
            TinyIntVector outS = null;
            for (String col : projected) {
                if ("n".equals(col)) {
                    Field f = Schemas.nullable("n", Schemas.INT64);
                    fields.add(f);
                    outN = (BigIntVector) f.createVector(alloc);
                    outN.allocateNew();
                    vectors.add(outN);
                } else if ("s".equals(col)) {
                    Field dictF = dictField("s");
                    fields.add(dictF);
                    // The data vector for a dict-encoded field is the int8
                    // index vector, not a VarCharVector.
                    Field idxF = new Field("s",
                            new FieldType(true, INDEX_TYPE, dictF.getDictionary()), null);
                    outS = (TinyIntVector) idxF.createVector(alloc);
                    outS.allocateNew();
                    vectors.add(outS);
                }
            }
            int dst = 0;
            for (int i = 0; i < nVals.length; i++) {
                if (!mask[i]) continue;
                if (outN != null) outN.setSafe(dst, nVals[i]);
                if (outS != null) outS.setSafe(dst, (byte) DICT_VALUES.indexOf(sVals[i]));
                dst++;
            }
            for (FieldVector v : vectors) v.setValueCount(kept);
            VectorSchemaRoot root = new VectorSchemaRoot(new Schema(fields), vectors, kept);

            DictionaryProvider.MapDictionaryProvider provider =
                    new DictionaryProvider.MapDictionaryProvider();
            if (outS != null) {
                VarCharVector dictVec = (VarCharVector) new Field("",
                        new FieldType(true, Schemas.UTF8, null), null).createVector(alloc);
                dictVec.allocateNew();
                for (int i = 0; i < DICT_VALUES.size(); i++) {
                    dictVec.setSafe(i, new Text(DICT_VALUES.get(i)));
                }
                dictVec.setValueCount(DICT_VALUES.size());
                provider.put(new Dictionary(dictVec,
                        new DictionaryEncoding(DICT_ID, false, INDEX_TYPE)));
                out.emit(root, null, provider);
            } else {
                out.emit(root);
            }
        }
    }
}
