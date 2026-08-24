// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateCombineRequest;
import farm.query.vgi.protocol.AggregateCombineResponse;
import farm.query.vgi.protocol.AggregateDestructorRequest;
import farm.query.vgi.protocol.AggregateDestructorResponse;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgi.protocol.AggregateUpdateResponse;
import farm.query.vgi.protocol.AttachCatalogInfo;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CardinalityRequest;
import farm.query.vgi.protocol.CardinalityResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogVersionResponse;
import farm.query.vgi.protocol.CopyFromContext;
import farm.query.vgi.protocol.CopyFromFormatInfo;
import farm.query.vgi.protocol.CopyToContext;
import farm.query.vgi.protocol.DynamicToStringResponse;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableBufferingCombineRequest;
import farm.query.vgi.protocol.TableBufferingCombineResponse;
import farm.query.vgi.protocol.TableBufferingDestructorRequest;
import farm.query.vgi.protocol.TableBufferingDestructorResponse;
import farm.query.vgi.protocol.TableBufferingProcessRequest;
import farm.query.vgi.protocol.TableBufferingProcessResponse;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgi.protocol.TransactionBeginResponse;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgi.generated.VgiProtocolSchemas;
import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every wire record must put on the wire exactly the schema the VGI protocol
 * defines for it.
 *
 * <h2>Why this test exists</h2>
 *
 * <p>Records reach the wire two ways here, and each fails differently. A
 * handful are written by a hand-built serialiser (nested
 * {@code list<list<int32>>} shapes, dictionary-encoded enums, and fields whose
 * wire nullability the generic derivation does not express); those have two
 * descriptions of one schema and can drift apart. The rest go through
 * vgi-rpc-java's {@code RecordCodec}, which derives the schema from the record
 * components — declaration and wire cannot disagree, because there is only one
 * of them, which is also why nothing in this SDK could ever catch that one
 * being <em>wrong</em>.</p>
 *
 * <p>So there are two axes. Hand-built records are compared against their
 * declaration AND against the protocol; codec-serialised records are compared
 * against the protocol, which for them is the only check there is. The protocol
 * side comes from {@link VgiProtocolSchemas}, generated from
 * {@code vgi/protocol.py} by {@code vgi.codegen.java_schemas} and regenerated
 * rather than edited.</p>
 *
 * <p>They drift silently and fail remotely. Three real examples, all found in
 * one week across the SDKs:</p>
 *
 * <ul>
 *   <li>{@code PlanResponse} declared two fields {@code @Nullable} that the
 *       protocol declared non-null at the time; the client rejected the entire
 *       response with "out-of-date Apache Arrow schema", naming nothing useful.
 *       The protocol has since been corrected and declares both nullable (see
 *       the vgi-rpc note below), but the failure mode is unchanged: any
 *       disagreement is rejected wholesale;</li>
 *   <li>a builder supplied 7 of a 9-field schema's columns. The missing one was
 *       a LIST, and Arrow dereferences a list's children while writing, so
 *       <em>every</em> multi-branch table died on
 *       {@code Cannot read properties of undefined} — including tables that
 *       predated the new fields entirely;</li>
 *   <li>a hand-written schema differed from the generated one on
 *       dictionary-versus-plain utf8, so a correct client was rejected at the
 *       first catalog call.</li>
 * </ul>
 *
 * <p>None of those is visible in a round-trip test written in the same SDK: the
 * reader and the writer share the mistake. What catches them is comparing the
 * bytes actually produced against the declaration, field by field, which is
 * what happens below. Writing it turned up the same nullability lie in three
 * records here — {@code FunctionInfo.max_workers}, both binary fields of
 * {@code MacroInfo}, and eight fields of {@code TableInfo} — plus three
 * {@code TableInfo} constraint lists declared as {@code int64} where the wire
 * carries {@code int32}. Every one was the declaration being wrong, not the
 * serialiser, so nothing on the wire moved; what changed is that the
 * declaration can now be trusted by anyone deriving a reader from it.</p>
 *
 * <p>Those same nullability fields have since moved again, in the other
 * direction, and for a reason neither axis above could see: the PROTOCOL was
 * wrong. vgi-rpc derived a field's nullability with
 * {@code get_origin(...) is UnionType}, which is {@code Annotated} — never a
 * union — for every field wrapped in {@code Annotated}, so 54 protocol fields
 * were described as non-null while their values were free to be, and routinely
 * were, null. No single SDK notices: the same wrong schema describes both the
 * write and the read. This test, comparing a peer's declaration field by
 * field, is what surfaced it. vgi-rpc 0.43.0 fixed the derivation, the
 * generated {@link VgiProtocolSchemas} now says nullable for all 54, and the
 * Java declarations and hand-built serialisers here were flipped to match. The
 * {@code ScanSplit} allowance list that used to excuse four of those fields is
 * gone with it — the comparison is strict on nullability everywhere.</p>
 *
 * <p>Adding the protocol axis turned up the rest of the same class, in records
 * the first axis is structurally blind to: {@code InitRequest} declared
 * {@code pushdown_filters}, {@code join_keys} and {@code split_tokens} as
 * {@code binary} where the protocol carries {@code large_binary}, and eleven of
 * its optional fields as non-null; {@code CatalogAttachRequest} was missing
 * {@code client_capabilities} entirely, so a Java worker could not see what
 * engine had attached to it; and nine more request records described optional
 * opaque-data and transaction columns as mandatory.</p>
 *
 * <h2>What the declaration means here</h2>
 *
 * <p>The expected schema is derived from the record components — names, order,
 * types, {@code @Nullable}, {@code @ArrowField} — by the same
 * {@link SchemaDerivation} the codec path uses, with one adjustment: a
 * component annotated {@link ArrowFieldType#DICT_INT16_UTF8} is expected to be
 * genuinely dictionary-encoded. The generic derivation flattens that annotation
 * to plain utf8 (the reader accepts either), but the catalog wire these
 * serialisers write uses a real {@code dictionary<int16, utf8>} and the C++
 * extension's generated schemas say so, so flattening it here would let exactly
 * the third failure above through. Dictionary <em>ids</em> are not compared:
 * the id only has to agree between a schema field and the dictionary batch
 * beside it in the same stream.</p>
 *
 * <h2>What it does not cover</h2>
 *
 * <p>Encoders whose Java type is an API shape rather than a wire mirror —
 * {@code SettingSpec}, {@code AttachOptionSpec}, {@code SecretTypeSpec},
 * {@code ColumnStatistics} — build columns by transforming their input (an
 * {@code ArrowType} becomes serialised schema bytes, a value becomes an encoded
 * default), so there is no declaration to compare a produced schema against.
 * Neither do the encoders with no Java record at all behind them
 * ({@code ScanBranchesResult} and the batch {@code table_function_plan}
 * receives, both read or written field by field), which are assembled straight
 * from their arguments. Those keep their own round-trip tests, and every
 * protocol schema not covered here is named with its reason in
 * {@code NOT_IMPLEMENTED_IN_JAVA} — the list is asserted exhaustive, so a
 * newly added protocol record cannot slip past by being written about
 * nowhere.</p>
 */
class WireRecordSchemaConformanceTest {

    /**
     * One hand-built wire record: how to serialise it, and the enum-valued
     * fields whose sample value has to be a member of the field's dictionary.
     */
    private record HandBuilt(Class<? extends Record> record,
                             String schemaName,
                             Function<Record, byte[]> serialise,
                             Map<String, Object> enumValues) {
        @Override
        public String toString() {
            return record.getSimpleName();
        }
    }

    private static List<HandBuilt> handBuilt() {
        return List.of(
                new HandBuilt(FunctionInfo.class, "FunctionInfo",
                        r -> FunctionInfoSerializer.serialize((FunctionInfo) r),
                        Map.of("function_type", "table",
                                "stability", "CONSISTENT",
                                "null_handling", "DEFAULT",
                                "order_preservation", "NO_ORDER_GUARANTEE",
                                "partition_kind", "NOT_PARTITIONED",
                                "order_dependent", "NOT_ORDER_DEPENDENT",
                                "distinct_dependent", "NOT_DISTINCT_DEPENDENT")),
                new HandBuilt(MacroInfo.class, "MacroInfo",
                        r -> MacroInfoSerializer.serialize((MacroInfo) r),
                        Map.of("macro_type", "scalar")),
                new HandBuilt(TableInfo.class, "TableInfo",
                        r -> TableInfoSerializer.serialize((TableInfo) r),
                        Map.of()),
                new HandBuilt(CopyFromFormatInfo.class, "CopyFromFormatInfo",
                        r -> CopyFromFormatInfoSerializer.serialize((CopyFromFormatInfo) r),
                        Map.of()),
                new HandBuilt(ScanSplit.class, "ScanSplit",
                        r -> ScanSplitSerializer.serialize((ScanSplit) r),
                        Map.of()));
    }

    /**
     * Whether column ORDER is part of the contract for a given record.
     *
     * <p>It depends on how the far end reads the batch, and the two ways differ.
     * A unary RESPONSE is checked by the C++ client with
     * {@code arrow::Schema::Equals} (see {@code ValidateResponseSchema} in
     * {@code vgi_schema_registry.cpp}), which compares fields positionally — a
     * response whose columns are in a different order is rejected outright, and
     * the same holds for the per-item schemas inside an {@code items} list. A
     * REQUEST, and the stream header {@code init} answers with, are read column
     * by column via {@code GetColumnByName}, so their order is not observable
     * and matching it would be churn with nothing behind it.</p>
     *
     * <p>So order is asserted where it is load-bearing and the field SET is
     * asserted everywhere. A record whose columns disagree in NAME still fails
     * either way — that is the missing-column case, which is fatal on both
     * paths.</p>
     */
    private enum Ordering {
        /** Column order is part of the contract (unary responses and list items). */
        ORDERED,
        /** Columns are read by name; only the field set and each field's shape matter. */
        BY_NAME
    }

    private static final Ordering ORDERED = Ordering.ORDERED;
    private static final Ordering BY_NAME = Ordering.BY_NAME;

    /** One codec-serialised record's protocol schema, and how strictly to compare it. */
    private record Codec(String schemaName, Ordering ordering) {
    }

    /**
     * Wire records whose schema comes from the record declaration itself, via
     * {@code RecordCodec}, paired with the name of the schema the protocol
     * defines for them.
     *
     * <p>Within this SDK these cannot drift, because the declaration <em>is</em>
     * the schema — which is also why nothing here could catch the declaration
     * being wrong. {@code PlanResponse} marked two components {@code @Nullable}
     * that the protocol then declared non-null and every test in this repo
     * passed; the C++ client rejected the whole response. So the comparison is
     * against {@link VgiProtocolSchemas}, generated from the protocol rather
     * than from Java, and the mapping is spelled out because it is not always
     * one word — a method's result schema is named after the method
     * ({@code CardinalityResponse} is {@code TableFunctionCardinalityResult}),
     * and a record that appears only as a nested column is addressed through
     * its parent ({@code FunctionInfo.examples[]}).</p>
     */
    private static Map<Class<? extends Record>, Codec> codecSerialised() {
        Map<Class<? extends Record>, Codec> m = new LinkedHashMap<>();
        m.put(AggregateBindRequest.class, new Codec("AggregateBindRequest", BY_NAME));
        m.put(AggregateBindResponse.class, new Codec("AggregateBindResult", ORDERED));
        m.put(AggregateCombineRequest.class, new Codec("AggregateCombineRequest", BY_NAME));
        m.put(AggregateCombineResponse.class, new Codec("AggregateCombineResult", ORDERED));
        m.put(AggregateDestructorRequest.class, new Codec("AggregateDestructorRequest", BY_NAME));
        m.put(AggregateDestructorResponse.class, new Codec("AggregateDestructorResult", ORDERED));
        m.put(AggregateFinalizeRequest.class, new Codec("AggregateFinalizeRequest", BY_NAME));
        m.put(AggregateFinalizeResponse.class, new Codec("AggregateFinalizeResult", ORDERED));
        m.put(AggregateUpdateRequest.class, new Codec("AggregateUpdateRequest", BY_NAME));
        m.put(AggregateUpdateResponse.class, new Codec("AggregateUpdateResult", ORDERED));
        m.put(AttachCatalogInfo.class, new Codec("AttachCatalogInfo", ORDERED));
        m.put(BindRequest.class, new Codec("BindRequest", BY_NAME));
        m.put(BindResponse.class, new Codec("BindResult", ORDERED));
        m.put(CardinalityRequest.class, new Codec("TableFunctionCardinalityRequest", BY_NAME));
        m.put(CardinalityResponse.class, new Codec("TableFunctionCardinalityResult", ORDERED));
        m.put(CatalogAttachRequest.class, new Codec("CatalogAttachRequest", BY_NAME));
        m.put(CatalogAttachResult.class, new Codec("CatalogAttachResult", ORDERED));
        m.put(CatalogVersionResponse.class, new Codec("CatalogVersionResult", ORDERED));
        m.put(CopyFromContext.class, new Codec("CopyFromContext", BY_NAME));
        m.put(CopyToContext.class, new Codec("CopyToContext", BY_NAME));
        m.put(DynamicToStringResponse.class, new Codec("TableFunctionDynamicToStringResult", ORDERED));
        m.put(FunctionExample.class, new Codec("FunctionInfo.examples[]", ORDERED));
        m.put(FunctionRequiredSecret.class, new Codec("FunctionInfo.required_secrets[]", ORDERED));
        m.put(GlobalInitResponse.class, new Codec("GlobalInitResponse", BY_NAME));
        m.put(InitRequest.class, new Codec("InitRequest", BY_NAME));
        // One Java record serves every `{items: list<binary>}` response; they
        // are the same schema under a dozen method names, so any one of them
        // is the comparison.
        m.put(ItemsResponse.class, new Codec("CatalogSchemasResult", ORDERED));
        m.put(PlanResponse.class, new Codec("TableFunctionPlanResult", ORDERED));
        m.put(SchemaInfo.class, new Codec("SchemaInfo", ORDERED));
        m.put(TableBufferingCombineRequest.class, new Codec("TableBufferingCombineRequest", BY_NAME));
        m.put(TableBufferingCombineResponse.class, new Codec("TableBufferingCombineResult", ORDERED));
        m.put(TableBufferingDestructorRequest.class, new Codec("TableBufferingDestructorRequest", BY_NAME));
        m.put(TableBufferingDestructorResponse.class, new Codec("TableBufferingDestructorResult", ORDERED));
        m.put(TableBufferingProcessRequest.class, new Codec("TableBufferingProcessRequest", BY_NAME));
        m.put(TableBufferingProcessResponse.class, new Codec("TableBufferingProcessResult", ORDERED));
        m.put(TableFunctionPlanRequest.class, new Codec("TableFunctionPlanRequest", BY_NAME));
        m.put(TableScanFunctionGetResponse.class, new Codec("ScanFunctionResult", ORDERED));
        m.put(TransactionBeginResponse.class, new Codec("CatalogTransactionBeginResult", ORDERED));
        m.put(ViewInfo.class, new Codec("ViewInfo", ORDERED));
        return m;
    }

    private static Stream<Object[]> codecCases() {
        return codecSerialised().entrySet().stream()
                .map(e -> new Object[] {e.getKey().getSimpleName(), e.getKey(), e.getValue()});
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("handBuilt")
    void handBuiltSchemaMatchesTheRecordDeclaration(HandBuilt subject) throws Exception {
        byte[] wire = subject.serialise().apply(sample(subject.record(), subject.enumValues()));

        List<String> problems = compare(subject.record().getSimpleName(),
                declaredSchema(subject.record()), producedSchema(wire), false, Ordering.ORDERED);

        assertTrue(problems.isEmpty(), () -> subject.record().getSimpleName()
                + ": the hand-built schema and the record declaration disagree.\n  "
                + String.join("\n  ", problems));
    }

    /**
     * The bytes a hand-built serialiser writes are the schema the protocol
     * defines.
     *
     * <p>The test above compares the serialiser against the record declaration,
     * which catches the two halves of this SDK disagreeing but not both being
     * wrong together. This one compares the same bytes against
     * {@link VgiProtocolSchemas}, so the loop closes: produced == declared ==
     * protocol.</p>
     *
     * @param subject the hand-built record under test.
     * @throws Exception if the serialiser or the Arrow reader fails.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("handBuilt")
    void handBuiltSchemaMatchesTheProtocol(HandBuilt subject) throws Exception {
        byte[] wire = subject.serialise().apply(sample(subject.record(), subject.enumValues()));

        List<String> problems = compare(subject.record().getSimpleName(),
                resolveProtocolSchema(subject.schemaName()), producedSchema(wire), false,
                Ordering.ORDERED);

        assertTrue(problems.isEmpty(), () -> subject.record().getSimpleName()
                + " wrote a schema the protocol does not define (protocol schema '"
                + subject.schemaName() + "').\n  " + String.join("\n  ", problems));
    }

    /**
     * A codec-serialised record's declaration is the schema the protocol
     * defines.
     *
     * <p>There is no second Java-side description to compare against here — the
     * codec derives the wire bytes from the same components — so this compares
     * against the protocol directly. It is the only check these records get,
     * and the one that would have caught {@code PlanResponse}.</p>
     *
     * @param label the record's simple name, for the test display name.
     * @param record the codec-serialised record under test.
     * @param codec the protocol schema it must match, and how strictly.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("codecCases")
    void codecSerialisedDeclarationMatchesTheProtocol(String label,
            Class<? extends Record> record, Codec codec) {
        Schema derived = SchemaDerivation.schemaForRecord(
                record.asSubclass(ArrowSerializableRecord.class));

        List<String> problems = compare(label, resolveProtocolSchema(codec.schemaName()), derived,
                true, codec.ordering());

        assertTrue(problems.isEmpty(), () -> label
                + ": the record declaration and the protocol disagree (protocol schema '"
                + codec.schemaName() + "'). The declaration is what RecordCodec puts on the wire, so a"
                + " mismatch here is a mismatch the far end sees.\n  "
                + String.join("\n  ", problems));
    }

    @Test
    void everyWireRecordIsClassified() {
        Set<Class<?>> byHand = handBuilt().stream()
                .map(HandBuilt::record).collect(Collectors.toSet());
        Set<Class<? extends Record>> byCodec = codecSerialised().keySet();

        List<String> unclassified = wireRecords().stream()
                .filter(r -> !byHand.contains(r) && !byCodec.contains(r))
                .map(Class::getSimpleName)
                .sorted()
                .toList();

        assertTrue(unclassified.isEmpty(), () -> "unclassified wire record(s) " + unclassified
                + ": add each to handBuilt() if the SDK writes its Arrow schema by hand — which is"
                + " the case that drifts — or to codecSerialised() if RecordCodec derives it from"
                + " the declaration. A record in neither list is a record nothing checks.");

        List<String> both = byHand.stream().filter(byCodec::contains)
                .map(Class::getSimpleName).sorted().toList();
        assertTrue(both.isEmpty(), () -> "listed as both hand-built and codec-serialised: " + both);

        Set<Class<?>> onTheWire = Set.copyOf(wireRecords());
        List<String> stale = Stream.concat(byHand.stream(), byCodec.stream())
                .filter(r -> !onTheWire.contains(r))
                .map(Class::getSimpleName).sorted().toList();
        assertTrue(stale.isEmpty(), () -> "listed but no longer a wire record: " + stale);
    }

    /**
     * Every generated protocol schema is either checked here or named as one
     * this SDK does not implement.
     *
     * <p>Without this, the way to make a failing row pass is to delete it, and
     * the way to skip a newly added protocol record is to write nothing at all.
     * The generated file is the full protocol; anything in it that no Java
     * record covers has to be declared as such, in writing.</p>
     */
    @Test
    void everyProtocolSchemaIsAccountedFor() {
        Set<String> checked = codecSerialised().values().stream()
                .map(Codec::schemaName).collect(Collectors.toCollection(java.util.HashSet::new));
        handBuilt().forEach(h -> checked.add(h.schemaName()));

        List<String> unaccounted = VgiProtocolSchemas.byName().keySet().stream()
                .filter(name -> !checked.contains(name))
                .filter(name -> !NOT_IMPLEMENTED_IN_JAVA.containsKey(name))
                .sorted()
                .toList();

        assertTrue(unaccounted.isEmpty(), () -> "protocol schema(s) " + unaccounted
                + " have no Java record checking them: map a record to each in codecSerialised()"
                + " or handBuilt(), or add it to NOT_IMPLEMENTED_IN_JAVA with the reason.");

        List<String> unknown = Stream.concat(checked.stream(), NOT_IMPLEMENTED_IN_JAVA.keySet().stream())
                .filter(name -> !name.contains("."))
                .filter(name -> !VgiProtocolSchemas.byName().containsKey(name))
                .sorted().toList();
        assertTrue(unknown.isEmpty(), () -> "named protocol schema(s) that do not exist: " + unknown
                + " — regenerate VgiProtocolSchemas.java, or fix the name.");
    }

    /**
     * Protocol schemas with no Java record behind them, and why.
     *
     * <p>Every entry is a decision, not an oversight. Most are the per-method
     * params envelopes, which vgi-rpc-java builds from the method signature
     * rather than from a record — there is no declaration to compare. The rest
     * are recorded individually.</p>
     */
    private static final Map<String, String> NOT_IMPLEMENTED_IN_JAVA = notImplementedInJava();

    private static Map<String, String> notImplementedInJava() {
        Map<String, String> m = new LinkedHashMap<>();
        String params = "a per-method params envelope; vgi-rpc-java derives it from the service"
                + " method signature, so there is no record declaration to compare against";
        VgiProtocolSchemas.byName().keySet().stream()
                .filter(n -> n.endsWith("Params"))
                .forEach(n -> m.put(n, params));
        m.put("CatalogInfo", "built by CatalogInfoSerializer from an API-shaped value rather than"
                + " a wire-mirroring record; covered by its own round-trip test");
        m.put("IndexInfo", "indexes are not implemented in this SDK");
        for (String streaming : List.of("AggregateStreamingOpenResult", "AggregateStreamingChunkResult",
                "AggregateStreamingCloseResult")) {
            m.put(streaming, "streaming-partitioned aggregates are not implemented in this SDK");
        }
        for (String window : List.of("AggregateWindowResult", "AggregateWindowBatchResult",
                "AggregateWindowInitResult", "AggregateWindowDestructorResult")) {
            m.put(window, "aggregate window functions are not implemented in this SDK");
        }
        m.put("TableFunctionPlanRequest", "read field-by-field with IpcUnpacker rather than through a"
                + " record (VgiServiceImpl.table_function_plan), so there is no declaration to compare."
                + " A field renamed or removed in the protocol surfaces here as a silently absent value,"
                + " not as an error — giving it a record would close that");
        m.put("CatalogIndexGetResult", "indexes are not implemented in this SDK");
        m.put("CatalogSchemaContentsIndexesResult", "indexes are not implemented in this SDK");
        m.put("ScanBranchesResult", "assembled straight from its arguments by"
                + " ScanBranchesResultSerializer, with no record behind it; has its own round-trip test");
        m.put("ScanBranch", "assembled straight from its arguments by ScanBranchesResultSerializer,"
                + " with no record behind it; has its own round-trip test");
        m.put("CatalogCatalogsResult", "same {items: list<binary>} shape as CatalogSchemasResult,"
                + " which ItemsResponse is checked against");
        m.put("CatalogCopyFromFormatsResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogMacroGetResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogSchemaContentsFunctionsResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogSchemaContentsMacrosResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogSchemaContentsTablesResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogSchemaContentsViewsResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogSchemaGetResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogTableGetResult", "same shape as CatalogSchemasResult (see above)");
        m.put("CatalogViewGetResult", "same shape as CatalogSchemasResult (see above)");
        return m;
    }

    /**
     * A protocol schema by name, or the struct behind a nested column.
     *
     * <p>{@code "FunctionInfo.examples[]"} is the struct a list item of
     * {@code FunctionInfo.examples} carries — the shape a record that appears
     * only nested has to match.</p>
     */
    private static Schema resolveProtocolSchema(String name) {
        int dot = name.indexOf('.');
        if (dot < 0) {
            return VgiProtocolSchemas.get(name);
        }
        Schema parent = VgiProtocolSchemas.get(name.substring(0, dot));
        String column = name.substring(dot + 1);
        boolean listItem = column.endsWith("[]");
        if (listItem) {
            column = column.substring(0, column.length() - 2);
        }
        for (Field field : parent.getFields()) {
            if (!field.getName().equals(column)) {
                continue;
            }
            Field target = listItem ? field.getChildren().get(0) : field;
            return new Schema(target.getChildren());
        }
        return fail("protocol schema '" + name.substring(0, dot) + "' has no column '" + column + "'");
    }

    // ------------------------------------------------------------------
    // Declared vs produced
    // ------------------------------------------------------------------

    /** The schema the record declaration describes, read at wire fidelity. */
    private static Schema declaredSchema(Class<? extends Record> record) {
        List<Field> fields = new ArrayList<>();
        for (RecordComponent rc : record.getRecordComponents()) {
            Field derived = SchemaDerivation.buildField(rc.getName(), rc.getGenericType(), rc);
            fields.add(asDeclaredOnTheWire(derived, rc));
        }
        return new Schema(fields);
    }

    private static Field asDeclaredOnTheWire(Field derived, RecordComponent rc) {
        ArrowField override = rc.getAnnotation(ArrowField.class);
        if (override == null || override.value() != ArrowFieldType.DICT_INT16_UTF8) {
            return derived;
        }
        DictionaryEncoding enc = new DictionaryEncoding(0L, false, new ArrowType.Int(16, true));
        return new Field(derived.getName(),
                new FieldType(derived.isNullable(), derived.getType(), enc),
                derived.getChildren());
    }

    /**
     * The schema the serialiser actually put on the wire.
     *
     * <p>A dictionary-encoded column comes back from Arrow-Java's reader
     * carrying the INDEX type ({@code int16}) where the wire schema names the
     * VALUE type ({@code utf8}) — the reader is describing the vector it built,
     * not the message it read. Comparing against that view would assert the
     * quirk instead of the contract and would accept a dictionary of the wrong
     * value type, so the value type is read back out of the dictionary batch
     * that travelled alongside and put where the field says it is.</p>
     */
    private static Schema producedSchema(byte[] wire) throws IOException {
        try (RootAllocator alloc = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(wire), alloc)) {
            if (!reader.loadNextBatch()) {
                return fail("the serialiser produced no record batch");
            }
            Map<Long, Dictionary> dictionaries = reader.getDictionaryVectors();
            return new Schema(reader.getVectorSchemaRoot().getSchema().getFields().stream()
                    .map(f -> withDictionaryValueType(f, dictionaries))
                    .toList());
        }
    }

    private static Field withDictionaryValueType(Field field, Map<Long, Dictionary> dictionaries) {
        List<Field> children = field.getChildren().stream()
                .map(child -> withDictionaryValueType(child, dictionaries))
                .toList();
        DictionaryEncoding enc = field.getDictionary();
        if (enc == null) {
            return new Field(field.getName(), field.getFieldType(), children);
        }
        Dictionary dictionary = dictionaries.get(enc.getId());
        if (dictionary == null) {
            return fail("field '" + field.getName() + "' is dictionary-encoded with id "
                    + enc.getId() + " but no dictionary with that id was written — the reader"
                    + " on the other end has nothing to resolve the indices against");
        }
        ArrowType valueType = dictionary.getVector().getField().getType();
        return new Field(field.getName(), new FieldType(field.isNullable(), valueType, enc), children);
    }

    /**
     * Every way the two schemas differ, as sentences naming the record and field.
     *
     * <p>{@code dictionaryMayBeFlatten} relaxes exactly one difference, for the
     * codec path only: the generic derivation writes a dictionary-encoded enum
     * column as plain utf8. Both ends read either encoding, so the wire is
     * interoperable; what still has to match is the dictionary's VALUE type,
     * which is what the field carries. The hand-built serialisers write real
     * dictionaries and are compared strictly — a hand-built schema that
     * flattened one is the drift that already rejected a correct client at its
     * first catalog call.</p>
     */
    private static List<String> compare(String record, Schema declared, Schema produced,
            boolean dictionaryMayBeFlattened, Ordering ordering) {
        List<String> problems = new ArrayList<>();
        List<Field> want = declared.getFields();
        List<Field> got = produced.getFields();

        List<String> wantNames = want.stream().map(Field::getName).toList();
        List<String> gotNames = got.stream().map(Field::getName).toList();

        if (ordering == Ordering.ORDERED) {
            if (!wantNames.equals(gotNames)) {
                // Reported as whole lists: a missing or reordered column shifts
                // every field after it, and a per-field diff would then read as a
                // dozen unrelated type errors rather than one dropped column.
                problems.add("column names/order: declared " + wantNames + " but wrote " + gotNames);
                return problems;
            }
            for (int i = 0; i < want.size(); i++) {
                compareField(record + "." + want.get(i).getName(), want.get(i), got.get(i), problems,
                        dictionaryMayBeFlattened);
            }
            return problems;
        }

        Map<String, Field> byName = got.stream()
                .collect(Collectors.toMap(Field::getName, Function.identity(), (a, b) -> a,
                        java.util.LinkedHashMap::new));
        List<String> missing = wantNames.stream().filter(n -> !byName.containsKey(n)).toList();
        List<String> unexpected = gotNames.stream().filter(n -> !wantNames.contains(n)).toList();
        if (!missing.isEmpty() || !unexpected.isEmpty()) {
            problems.add("columns: declared " + wantNames + " but wrote " + gotNames
                    + (missing.isEmpty() ? "" : " (missing " + missing + ")")
                    + (unexpected.isEmpty() ? "" : " (unexpected " + unexpected + ")"));
            return problems;
        }
        for (Field wantField : want) {
            compareField(record + "." + wantField.getName(), wantField, byName.get(wantField.getName()),
                    problems, dictionaryMayBeFlattened);
        }
        return problems;
    }

    private static void compareField(String path, Field want, Field got, List<String> problems,
            boolean dictionaryMayBeFlattened) {
        if (!want.getType().equals(got.getType())) {
            problems.add(path + ": declared " + want.getType() + " but wrote " + got.getType());
        }
        if (want.isNullable() != got.isNullable()) {
            problems.add(path + ": declared nullable=" + want.isNullable()
                    + " but wrote nullable=" + got.isNullable());
        }
        String wantDict = describeDictionary(want.getDictionary());
        String gotDict = describeDictionary(got.getDictionary());
        boolean flattened = dictionaryMayBeFlattened
                && want.getDictionary() != null && got.getDictionary() == null;
        if (!wantDict.equals(gotDict) && !flattened) {
            problems.add(path + ": declared " + wantDict + " but wrote " + gotDict);
        }
        List<Field> wantChildren = want.getChildren();
        List<Field> gotChildren = got.getChildren();
        if (wantChildren.size() != gotChildren.size()) {
            problems.add(path + ": declared " + wantChildren.size() + " child field(s) but wrote "
                    + gotChildren.size());
            return;
        }
        for (int i = 0; i < wantChildren.size(); i++) {
            Field wantChild = wantChildren.get(i);
            Field gotChild = gotChildren.get(i);
            if (!wantChild.getName().equals(gotChild.getName())) {
                problems.add(path + ": declared child '" + wantChild.getName()
                        + "' but wrote '" + gotChild.getName() + "'");
                continue;
            }
            compareField(path + "." + wantChild.getName(), wantChild, gotChild, problems,
                    dictionaryMayBeFlattened);
        }
    }

    /** Dictionary encoding without its id — see the class documentation. */
    private static String describeDictionary(DictionaryEncoding enc) {
        return enc == null ? "no dictionary encoding"
                : "dictionary<" + enc.getIndexType() + ", ordered=" + enc.isOrdered() + ">";
    }

    // ------------------------------------------------------------------
    // Sample instances
    // ------------------------------------------------------------------

    /**
     * Build one instance of a record from its canonical constructor.
     *
     * <p>Reflective rather than hand-written so a component added to a record
     * is carried into the sample without anyone remembering to update it — the
     * whole point being to notice a new field, not to be told about it.</p>
     */
    private static Record sample(Class<? extends Record> record, Map<String, Object> enumValues)
            throws ReflectiveOperationException {
        RecordComponent[] components = record.getRecordComponents();
        Class<?>[] types = new Class<?>[components.length];
        Object[] args = new Object[components.length];
        for (int i = 0; i < components.length; i++) {
            types[i] = components[i].getType();
            args[i] = enumValues.containsKey(components[i].getName())
                    ? enumValues.get(components[i].getName())
                    : sampleValue(components[i]);
        }
        Constructor<? extends Record> ctor = record.getDeclaredConstructor(types);
        return ctor.newInstance(args);
    }

    private static Object sampleValue(RecordComponent component) {
        Class<?> type = component.getType();
        if (type == String.class) {
            return "sample";
        }
        if (type == byte[].class) {
            return new byte[0];
        }
        if (type == boolean.class || type == Boolean.class) {
            return Boolean.FALSE;
        }
        if (type == long.class || type == Long.class) {
            return 1L;
        }
        if (type == int.class || type == Integer.class) {
            return 1;
        }
        if (type == double.class || type == Double.class) {
            return 1.0d;
        }
        if (List.class.isAssignableFrom(type)) {
            return List.of();
        }
        if (Map.class.isAssignableFrom(type)) {
            return Map.of();
        }
        return fail("no sample value for " + component.getDeclaringRecord().getSimpleName() + "."
                + component.getName() + " of type " + type.getName()
                + " — extend sampleValue(), or pass one through the case's enumValues map"
                + " if the field is dictionary-encoded and only accepts known members");
    }

    // ------------------------------------------------------------------
    // Discovery
    // ------------------------------------------------------------------

    /** Every record declared in the wire-record package. */
    private static List<Class<?>> wireRecords() {
        String pkg = ScanSplit.class.getPackageName();
        URL located = WireRecordSchemaConformanceTest.class.getClassLoader()
                .getResource(pkg.replace('.', '/'));
        if (located == null || !"file".equals(located.getProtocol())) {
            return fail("cannot enumerate " + pkg + " from " + located
                    + "; this test must run against class files on disk, or a newly added"
                    + " wire record would go unnoticed — which is the failure it exists to catch");
        }
        try (Stream<Path> entries = Files.list(Path.of(located.toURI()))) {
            List<Class<?>> records = new ArrayList<>();
            for (Path p : entries.toList()) {
                String file = p.getFileName().toString();
                if (!file.endsWith(".class") || file.contains("$") || file.startsWith("package-info")) {
                    continue;
                }
                Class<?> cls = Class.forName(pkg + "." + file.substring(0, file.length() - 6));
                if (cls.isRecord()) {
                    records.add(cls);
                }
            }
            return records;
        } catch (IOException | URISyntaxException | ClassNotFoundException e) {
            return fail("enumerating " + pkg + ": " + e);
        }
    }
}
