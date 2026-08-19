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
import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every wire record whose Arrow schema this SDK writes by hand must produce
 * exactly the schema its record declaration describes.
 *
 * <h2>Why this test exists</h2>
 *
 * <p>Most of {@code farm.query.vgi.protocol} reaches the wire through
 * vgi-rpc-java's {@code RecordCodec}, which derives the schema from the record
 * components — declaration and wire cannot disagree, because there is only one
 * of them. A handful of records are written by a hand-built serialiser instead
 * (nested {@code list<list<int32>>} shapes, dictionary-encoded enums, and
 * fields whose wire nullability the generic derivation does not express), and
 * for those there are two descriptions of one schema, free to drift.</p>
 *
 * <p>They drift silently and fail remotely. Three real examples, all found in
 * one week across the SDKs:</p>
 *
 * <ul>
 *   <li>{@code PlanResponse} declared two fields {@code @Nullable} that the
 *       protocol declares non-null; the client rejected the entire response
 *       with "out-of-date Apache Arrow schema", naming nothing useful;</li>
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
 * ({@code ScanFunctionResult}, {@code ScanBranchesResult}), which are assembled
 * straight from their arguments. Those keep their own round-trip tests.</p>
 */
class WireRecordSchemaConformanceTest {

    /**
     * One hand-built wire record: how to serialise it, and the enum-valued
     * fields whose sample value has to be a member of the field's dictionary.
     */
    private record HandBuilt(Class<? extends Record> record,
                             Function<Record, byte[]> serialise,
                             Map<String, Object> enumValues) {
        @Override
        public String toString() {
            return record.getSimpleName();
        }
    }

    private static List<HandBuilt> handBuilt() {
        return List.of(
                new HandBuilt(FunctionInfo.class,
                        r -> FunctionInfoSerializer.serialize((FunctionInfo) r),
                        Map.of("function_type", "table",
                                "stability", "CONSISTENT",
                                "null_handling", "DEFAULT",
                                "order_preservation", "NO_ORDER_GUARANTEE",
                                "partition_kind", "NOT_PARTITIONED",
                                "order_dependent", "NOT_ORDER_DEPENDENT",
                                "distinct_dependent", "NOT_DISTINCT_DEPENDENT")),
                new HandBuilt(MacroInfo.class,
                        r -> MacroInfoSerializer.serialize((MacroInfo) r),
                        Map.of("macro_type", "scalar")),
                new HandBuilt(TableInfo.class,
                        r -> TableInfoSerializer.serialize((TableInfo) r),
                        Map.of()),
                new HandBuilt(CopyFromFormatInfo.class,
                        r -> CopyFromFormatInfoSerializer.serialize((CopyFromFormatInfo) r),
                        Map.of()),
                new HandBuilt(ScanSplit.class,
                        r -> ScanSplitSerializer.serialize((ScanSplit) r),
                        Map.of()));
    }

    /**
     * Wire records whose schema comes from the record declaration itself, via
     * {@code RecordCodec}. There is no second copy of the schema for these, so
     * there is nothing here that can drift — they are listed only so that a
     * newly added record is forced into one bucket or the other rather than
     * quietly escaping the check.
     */
    private static final Set<Class<?>> CODEC_SERIALISED = Set.of(
            AggregateBindRequest.class, AggregateBindResponse.class,
            AggregateCombineRequest.class, AggregateCombineResponse.class,
            AggregateDestructorRequest.class, AggregateDestructorResponse.class,
            AggregateFinalizeRequest.class, AggregateFinalizeResponse.class,
            AggregateUpdateRequest.class, AggregateUpdateResponse.class,
            AttachCatalogInfo.class, BindRequest.class, BindResponse.class,
            CardinalityRequest.class, CardinalityResponse.class,
            CatalogAttachRequest.class, CatalogAttachResult.class,
            CatalogVersionResponse.class, CopyFromContext.class, CopyToContext.class,
            DynamicToStringResponse.class, FunctionExample.class,
            FunctionRequiredSecret.class, GlobalInitResponse.class, InitRequest.class,
            ItemsResponse.class, PlanResponse.class, SchemaInfo.class,
            TableBufferingCombineRequest.class, TableBufferingCombineResponse.class,
            TableBufferingDestructorRequest.class, TableBufferingDestructorResponse.class,
            TableBufferingProcessRequest.class, TableBufferingProcessResponse.class,
            TableScanFunctionGetResponse.class, TransactionBeginResponse.class,
            ViewInfo.class);

    @ParameterizedTest(name = "{0}")
    @MethodSource("handBuilt")
    void handBuiltSchemaMatchesTheRecordDeclaration(HandBuilt subject) throws Exception {
        byte[] wire = subject.serialise().apply(sample(subject.record(), subject.enumValues()));

        List<String> problems = compare(subject.record().getSimpleName(),
                declaredSchema(subject.record()), producedSchema(wire));

        assertTrue(problems.isEmpty(), () -> subject.record().getSimpleName()
                + ": the hand-built schema and the record declaration disagree.\n  "
                + String.join("\n  ", problems));
    }

    @Test
    void everyWireRecordIsClassified() {
        Set<Class<?>> byHand = handBuilt().stream()
                .map(HandBuilt::record).collect(Collectors.toSet());

        List<String> unclassified = wireRecords().stream()
                .filter(r -> !byHand.contains(r) && !CODEC_SERIALISED.contains(r))
                .map(Class::getSimpleName)
                .sorted()
                .toList();

        assertTrue(unclassified.isEmpty(), () -> "unclassified wire record(s) " + unclassified
                + ": add each to handBuilt() if the SDK writes its Arrow schema by hand — which is"
                + " the case that drifts — or to CODEC_SERIALISED if RecordCodec derives it from"
                + " the declaration. A record in neither list is a record nothing checks.");

        List<String> both = byHand.stream().filter(CODEC_SERIALISED::contains)
                .map(Class::getSimpleName).sorted().toList();
        assertTrue(both.isEmpty(), () -> "listed as both hand-built and codec-serialised: " + both);

        Set<Class<?>> onTheWire = Set.copyOf(wireRecords());
        List<String> stale = Stream.concat(byHand.stream(), CODEC_SERIALISED.stream())
                .filter(r -> !onTheWire.contains(r))
                .map(Class::getSimpleName).sorted().toList();
        assertTrue(stale.isEmpty(), () -> "listed but no longer a wire record: " + stale);
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

    /** Every way the two schemas differ, as sentences naming the record and field. */
    private static List<String> compare(String record, Schema declared, Schema produced) {
        List<String> problems = new ArrayList<>();
        List<Field> want = declared.getFields();
        List<Field> got = produced.getFields();

        List<String> wantNames = want.stream().map(Field::getName).toList();
        List<String> gotNames = got.stream().map(Field::getName).toList();
        if (!wantNames.equals(gotNames)) {
            // Reported as whole lists: a missing or reordered column shifts
            // every field after it, and a per-field diff would then read as a
            // dozen unrelated type errors rather than one dropped column.
            problems.add("column names/order: declared " + wantNames + " but wrote " + gotNames);
            return problems;
        }
        for (int i = 0; i < want.size(); i++) {
            compareField(record + "." + want.get(i).getName(), want.get(i), got.get(i), problems);
        }
        return problems;
    }

    private static void compareField(String path, Field want, Field got, List<String> problems) {
        if (!want.getType().equals(got.getType())) {
            problems.add(path + ": declared " + want.getType() + " but wrote " + got.getType());
        }
        if (want.isNullable() != got.isNullable()) {
            problems.add(path + ": declared nullable=" + want.isNullable()
                    + " but wrote nullable=" + got.isNullable());
        }
        String wantDict = describeDictionary(want.getDictionary());
        String gotDict = describeDictionary(got.getDictionary());
        if (!wantDict.equals(gotDict)) {
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
            compareField(path + "." + wantChild.getName(), wantChild, gotChild, problems);
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
