// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

/**
 * {@code cache_types(rows)} — nested / wide / NULL cacheable result.
 *
 * <p>Every other cacheable fixture emits flat int64, so the disk blob and the
 * streaming table-of-contents (seek-past-payload) path is only exercised on
 * fixed-width columns. This one emits STRUCT / LIST / DECIMAL / TIMESTAMP /
 * string columns with interleaved NULLs — validity bitmaps plus variable and
 * nested buffers — across many batches, so a spilled and streamed serve must
 * reassemble all of that byte-identically, not just a matching COUNT.
 *
 * <p>Row {@code i} is deterministic: {@code id=i}, and every 5th row is NULL in
 * each nullable column. Mirrors vgi-python's {@code CacheTypesFunction}.
 */
public final class CacheTypesFunction extends SimpleTableFunction {

    private static final ArrowType DECIMAL_18_2 = new ArrowType.Decimal(18, 2, 128);
    private static final ArrowType TIMESTAMP_US = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);

    private static final Field ATTRS = new Field("attrs",
            FieldType.nullable(new ArrowType.Struct()),
            List.of(Schemas.nullable("x", Schemas.INT64), Schemas.nullable("y", Schemas.UTF8)));

    private static final Schema OUTPUT = Schemas.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.list("tags", Schemas.INT64, true),
            ATTRS,
            Schemas.nullable("amt", DECIMAL_18_2),
            Schemas.nullable("ts", TIMESTAMP_US),
            Schemas.nullable("label", Schemas.UTF8));

    private static final int BATCH_SIZE = 2048;

    private static final FunctionSpec SPEC = FunctionSpec.builder("cache_types")
            .metadata(FunctionMetadata.describe(
                    "Nested/wide/NULL cacheable result (STRUCT/LIST/DECIMAL/TIMESTAMP + NULLs)")
                    .withCategories("generator", "cache", "testing"))
            .constArg("rows", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override protected Schema outputSchema() { return OUTPUT; }

    @Override public TableProducerState createProducer(TableInitParams p) {
        return new State(ParameterExtractor.of(p.arguments()).positional(0, "rows").asLong().required());
    }

    /** Countdown state over the requested row range. */
    public static final class State extends TableProducerState {
        /** Rows still to emit. */
        public long remaining;
        /** Index of the next row to emit. */
        public long currentIndex;

        /** Required no-arg constructor for state deserialization. */
        public State() {}

        State(long remaining) { this.remaining = remaining; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int size = (int) Math.min(remaining, BATCH_SIZE);
            Map<String, String> md = currentIndex == 0
                    ? CacheControl.ttl(CacheFunctions.DEFAULT_TTL_SECONDS).toMetadata() : null;

            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
            boolean emitted = false;
            try {
                root.allocateNew();
                fill(root, currentIndex, size);
                root.setRowCount(size);
                if (md == null) out.emit(root); else out.emit(root, md);
                emitted = true;
            } finally {
                if (!emitted) root.close();
            }
            currentIndex += size;
            remaining -= size;
        }
    }

    private static void fill(VectorSchemaRoot root, long base, int size) {
        BigIntVector id = (BigIntVector) root.getVector("id");
        ListVector tags = (ListVector) root.getVector("tags");
        StructVector attrs = (StructVector) root.getVector("attrs");
        DecimalVector amt = (DecimalVector) root.getVector("amt");
        TimeStampMicroVector ts = (TimeStampMicroVector) root.getVector("ts");
        VarCharVector label = (VarCharVector) root.getVector("label");

        BigIntVector attrX = (BigIntVector) attrs.getChild("x");
        VarCharVector attrY = (VarCharVector) attrs.getChild("y");
        UnionListWriter tagsWriter = tags.getWriter();

        for (int i = 0; i < size; i++) {
            long j = base + i;
            id.setSafe(i, j);
            if (j % 5 == 0) {
                tagsWriter.setPosition(i);
                tagsWriter.writeNull();
                attrs.setNull(i);
                amt.setNull(i);
                ts.setNull(i);
                label.setNull(i);
                continue;
            }
            tagsWriter.setPosition(i);
            tagsWriter.startList();
            for (long t = j; t <= j + 2; t++) tagsWriter.bigInt().writeBigInt(t);
            tagsWriter.endList();

            attrs.setIndexDefined(i);
            attrX.setSafe(i, j);
            attrY.setSafe(i, new Text("y" + j));

            // Decimal(18,2) rendering of Python's Decimal(f"{j}.{j % 100:02d}").
            amt.setSafe(i, new BigDecimal(j + "." + String.format("%02d", j % 100))
                    .setScale(2, RoundingMode.UNNECESSARY));
            ts.setSafe(i, j);
            label.setSafe(i, new Text("label-" + j));
        }
        tagsWriter.setValueCount(size);
        attrs.setValueCount(size);
    }
}
