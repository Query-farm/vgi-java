// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.AttachCatalogInfo;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.DynamicToStringResponse;
import farm.query.vgi.protocol.ForeignKeyInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableBufferingCombineResponse;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A worker-emitted record whose list / map component is {@code null} must still
 * put a non-null value in that column when the column is non-nullable.
 *
 * <p>These records are written by vgi-rpc-java's {@code RecordCodec}, which
 * writes a Java {@code null} as an Arrow null whatever the field's declared
 * nullability. The protocol (vgi-python's schemas) declares every one of these
 * list / map columns non-nullable, so a {@code new BindResponse(schema, null,
 * null, null, null)} from user code produced a batch a strict client rejects.
 * The records now read a {@code null} collection as empty.</p>
 */
class WireNonNullCollectionsTest {

    private static final Map<String, Class<? extends ArrowSerializableRecord>> RECORDS = Map.ofEntries(
            Map.entry("AttachCatalogInfo", AttachCatalogInfo.class),
            Map.entry("BindResponse", BindResponse.class),
            Map.entry("CatalogAttachResult", CatalogAttachResult.class),
            Map.entry("DynamicToStringResponse", DynamicToStringResponse.class),
            Map.entry("ForeignKeyInfo", ForeignKeyInfo.class),
            Map.entry("ItemsResponse", ItemsResponse.class),
            Map.entry("PlanResponse", PlanResponse.class),
            Map.entry("SchemaContents", SchemaContents.class),
            Map.entry("SchemaInfo", SchemaInfo.class),
            Map.entry("TableBufferingCombineResponse", TableBufferingCombineResponse.class),
            Map.entry("TableScanFunctionGetResponse", TableScanFunctionGetResponse.class),
            Map.entry("ViewInfo", ViewInfo.class));

    @ParameterizedTest
    @ValueSource(strings = {
            "AttachCatalogInfo", "BindResponse", "CatalogAttachResult",
            "DynamicToStringResponse", "ForeignKeyInfo", "ItemsResponse", "PlanResponse",
            "SchemaContents", "SchemaInfo", "TableBufferingCombineResponse",
            "TableScanFunctionGetResponse", "ViewInfo"})
    void nullCollectionsAreWrittenAsEmpty(String name) throws Exception {
        Class<? extends ArrowSerializableRecord> cls = RECORDS.get(name);
        byte[] ipc = RecordCodec.serializeToBytes(withNullCollections(cls));
        List<String> checked = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), Allocators.root())) {
            assertTrue(reader.loadNextBatch());
            for (FieldVector v : reader.getVectorSchemaRoot().getFieldVectors()) {
                ArrowType.ArrowTypeID id = v.getField().getType().getTypeID();
                boolean collection = id == ArrowType.ArrowTypeID.List || id == ArrowType.ArrowTypeID.Map;
                if (!collection || v.getField().isNullable()) continue;
                checked.add(v.getField().getName());
                assertFalse(v.isNull(0), name + "." + v.getField().getName()
                        + " is non-nullable but a null component was written as null");
            }
        }
        assertFalse(checked.isEmpty(), name + " has no non-nullable list/map column to check");
    }

    /**
     * The record with every List / Map component {@code null}, every other
     * reference component a harmless non-null value, and primitives zero.
     */
    private static ArrowSerializableRecord withNullCollections(Class<? extends ArrowSerializableRecord> cls)
            throws ReflectiveOperationException {
        RecordComponent[] comps = cls.getRecordComponents();
        Class<?>[] types = new Class<?>[comps.length];
        Object[] args = new Object[comps.length];
        for (int i = 0; i < comps.length; i++) {
            Class<?> t = comps[i].getType();
            types[i] = t;
            if (List.class.isAssignableFrom(t) || Map.class.isAssignableFrom(t)) args[i] = null;
            else if (t == boolean.class) args[i] = false;
            else if (t == long.class) args[i] = 0L;
            else if (t == int.class) args[i] = 0;
            else if (t == String.class) args[i] = comps[i].getName().equals("security") ? "regular" : "";
            else if (t.isEnum()) args[i] = t.getEnumConstants()[0];
            else if (t == byte[].class) args[i] = new byte[0];
            else args[i] = null;
        }
        Constructor<? extends ArrowSerializableRecord> ctor = cls.getDeclaredConstructor(types);
        return ctor.newInstance(args);
    }
}
