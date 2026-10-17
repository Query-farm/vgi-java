// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Pins the wire spelling of the dictionary-encoded enum columns this SDK
 * writes in catalog metadata.
 *
 * <p>A dictionary-encoded enum column carries the vgi-python enum member
 * NAME: {@code function_type} is one of {@code SCALAR}, {@code TABLE},
 * {@code AGGREGATE}, {@code TABLE_BUFFERING} ({@code vgi.metadata.CatalogFunctionType}
 * / {@code vgi.catalog.catalog_interface.FunctionType}), and {@code macro_type}
 * is {@code SCALAR} or {@code TABLE} ({@code MacroType}). This SDK used to send
 * the lower-case enum <em>values</em> ({@code "scalar"}, {@code "table"}, ...),
 * which the old C++ client happened to accept case-insensitively; the generated
 * strict decoders reject them, so every catalog enumeration against a Java
 * worker would fail.</p>
 *
 * <p>The checks read the values out of the actual IPC bytes -- both the decoded
 * row value and the dictionary batch itself -- so neither a lower-case
 * dictionary entry that happens to be unused nor a lower-case value can slip
 * through.</p>
 */
class WireEnumNamesTest {

    private static final List<String> FUNCTION_TYPE_NAMES =
            List.of("SCALAR", "TABLE", "AGGREGATE", "TABLE_BUFFERING");
    private static final List<String> MACRO_TYPE_NAMES = List.of("SCALAR", "TABLE");

    private static final org.apache.arrow.vector.types.pojo.Schema OUT =
            Schemas.of(Schemas.nullable("n", Schemas.INT64));

    static final class Sc extends ScalarFn {
        @Override public String name() { return "enum_scalar"; }
        @Override public String description() { return "doc"; }

        public void compute(@Vector BigIntVector v, BigIntVector result) {
            for (int i = 0; i < v.getValueCount(); i++) result.setSafe(i, v.get(i));
        }
    }

    static final class Tb implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("enum_table").description("doc").build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUT));
        }
        @Override public TableProducerState createProducer(TableInitParams params) {
            throw new UnsupportedOperationException("listed, never scanned");
        }
    }

    static final class Tio implements TableInOutFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("enum_tio").description("doc").build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUT));
        }
        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            throw new UnsupportedOperationException("listed, never run");
        }
    }

    static final class Agg implements AggregateFunction<long[]> {
        private static final FunctionSpec SPEC = FunctionSpec.builder("enum_agg").description("doc").build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public org.apache.arrow.vector.types.pojo.Schema outputSchema() { return OUT; }
        @Override public long[] newState() { return new long[1]; }
        @Override public void update(Map<Long, long[]> states, long[] groupIds, VectorSchemaRoot input) {}
        @Override public void combine(long[] target, long[] source) {}
        @Override public void finalize(FieldVector result, int rowIndex, long[] state) {}
    }

    static final class Buf implements TableBufferingFunction {
        @Override public String name() { return "enum_buffering"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("doc"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUT));
        }
        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            throw new UnsupportedOperationException();
        }
        @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
            throw new UnsupportedOperationException();
        }
        @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
            throw new UnsupportedOperationException();
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    private static byte[] attach(VgiServiceImpl svc, String catalog) {
        return svc.catalog_attach(CatalogAttachRequest.of(catalog, null, null, null), null)
                .attach_opaque_data();
    }

    @Test
    void functionTypeIsTheEnumMemberName() throws IOException {
        VgiServiceImpl svc = service(Worker.builder()
                .catalogName("enum_catalog")
                .registerScalar("main", new Sc())
                .registerTable("main", new Tb())
                .registerTableInOut("main", new Tio())
                .registerAggregate("main", new Agg())
                .registerTableBuffering("main", new Buf()));
        byte[] attach = attach(svc, "enum_catalog");

        Map<String, String> byName = new TreeMap<>();
        for (byte[] item : svc.catalog_schema_contents_functions(attach, List.of("main"), null, null, null)
                .items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            byName.put(info.name(), info.function_type());
            assertEquals(FUNCTION_TYPE_NAMES, dictionaryValues(item, "function_type"),
                    "function_type dictionary of " + info.name());
        }
        Map<String, String> expected = new TreeMap<>(Map.of(
                "enum_scalar", "SCALAR",
                "enum_table", "TABLE",
                // A streaming table-in-out function registers as a table
                // function; there is no TABLE_IN_OUT member.
                "enum_tio", "TABLE",
                "enum_agg", "AGGREGATE",
                "enum_buffering", "TABLE_BUFFERING"));
        assertEquals(expected, byName);
    }

    @Test
    void functionListingFilterAcceptsSchemaObjectTypeNames() {
        // The client filters catalog_schema_contents_functions with a
        // SchemaObjectType member name.
        VgiServiceImpl svc = service(Worker.builder()
                .catalogName("enum_catalog")
                .registerScalar("main", new Sc())
                .registerTable("main", new Tb())
                .registerAggregate("main", new Agg()));
        byte[] attach = attach(svc, "enum_catalog");
        assertEquals(List.of("enum_scalar"), listed(svc, attach, "SCALAR_FUNCTION"));
        assertEquals(List.of("enum_table"), listed(svc, attach, "TABLE_FUNCTION"));
        assertEquals(List.of("enum_agg"), listed(svc, attach, "AGGREGATE_FUNCTION"));
    }

    @Test
    void macroTypeIsTheEnumMemberName() throws IOException {
        VgiServiceImpl svc = service(Worker.builder()
                .catalogName("enum_catalog")
                .registerMacro(new Macro("main", "enum_scalar_macro", MacroType.SCALAR,
                        List.of("x"), "x + 1", null))
                .registerMacro(new Macro("main", "enum_table_macro", MacroType.TABLE,
                        List.of(), "SELECT 1", null)));
        byte[] attach = attach(svc, "enum_catalog");

        Map<String, String> byName = new TreeMap<>();
        for (String filter : List.of("SCALAR_MACRO", "TABLE_MACRO")) {
            for (byte[] item : svc.catalog_schema_contents_macros(attach, List.of("main"), filter, null, null)
                    .items()) {
                MacroInfo info = RecordCodec.deserializeFromBytes(item, MacroInfo.class);
                byName.put(info.name(), info.macro_type());
                assertEquals(MACRO_TYPE_NAMES, dictionaryValues(item, "macro_type"));
            }
        }
        assertEquals(Map.of("enum_scalar_macro", "SCALAR", "enum_table_macro", "TABLE"), byName);
    }

    @Test
    void handBuiltLowerCaseValuesAreCanonicalisedOnTheWire() throws IOException {
        // A CatalogInterface implementation builds its own MacroInfo /
        // FunctionInfo records; one written against the old lower-case spelling
        // must still put the member name on the wire.
        byte[] macro = MacroInfoSerializer.serialize(new MacroInfo(null, Map.of(), "m", "main", "table",
                List.of(), null, "SELECT 1", null));
        assertEquals("TABLE", RecordCodec.deserializeFromBytes(macro, MacroInfo.class).macro_type());
        assertEquals(MACRO_TYPE_NAMES, dictionaryValues(macro, "macro_type"));

        FunctionInfo scalar = VgiServiceImpl.scalarFunctionInfo(new Sc(), "main");
        FunctionInfo lower = new FunctionInfo(
                scalar.comment(), scalar.tags(), scalar.name(), scalar.schema_path(), "scalar",
                scalar.arguments(), scalar.output_schema(), scalar.parameter_default_values(),
                scalar.stability(), scalar.null_handling(), scalar.argument_monotonicity(),
                scalar.description(), scalar.examples(), scalar.categories(), scalar.projection_pushdown(),
                scalar.filter_pushdown(), scalar.sampling_pushdown(), scalar.late_materialization(),
                scalar.filter_semantic_profiles(), scalar.additional_filter_functions(),
                scalar.runtime_filter_algorithms(), scalar.filter_evaluation_contexts(),
                scalar.order_preservation(), scalar.max_workers(), scalar.supports_batch_index(),
                scalar.supports_splits(), scalar.filters_exactly_applied(),
                scalar.supports_positions(), scalar.split_token_ttl_seconds(),
                scalar.partition_kind(), scalar.order_dependent(), scalar.distinct_dependent(),
                scalar.supports_window(), scalar.streaming_partitioned(), scalar.has_finalize(),
                scalar.source_order_dependent(), scalar.sink_order_dependent(),
                scalar.requires_input_batch_index(), scalar.input_from_args(),
                scalar.required_settings(), scalar.required_secrets());
        byte[] fn = FunctionInfoSerializer.serialize(lower);
        assertEquals("SCALAR", RecordCodec.deserializeFromBytes(fn, FunctionInfo.class).function_type());
    }

    private static List<String> listed(VgiServiceImpl svc, byte[] attach, String type) {
        return svc.catalog_schema_contents_functions(attach, List.of("main"), type, null, null).items().stream()
                .map(b -> RecordCodec.deserializeFromBytes(b, FunctionInfo.class).name())
                .toList();
    }

    /** The values of {@code field}'s dictionary, exactly as the IPC stream carries them. */
    private static List<String> dictionaryValues(byte[] ipc, String field) throws IOException {
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), Allocators.root())) {
            reader.loadNextBatch();
            var encoding = reader.getVectorSchemaRoot().getSchema().findField(field).getDictionary();
            assertNotNull(encoding, field + " must be dictionary-encoded");
            Dictionary dict = reader.getDictionaryVectors().get(encoding.getId());
            VarCharVector values = (VarCharVector) dict.getVector();
            List<String> out = new ArrayList<>();
            for (int i = 0; i < values.getValueCount(); i++) {
                out.add(new String(values.get(i), StandardCharsets.UTF_8));
            }
            return out;
        }
    }
}
