// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi;

import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.SchemaDerivation;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgirpc.wire.IpcStreamWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Shared collection codec for wire 2.2 catalog metadata. */
public final class MetadataCollections {
    private MetadataCollections() {}

    /** Metadata records retain annotated dictionary columns in collection output. */
    public static <T extends ArrowSerializableRecord> Schema schema(Class<T> type) {
        Schema plain = SchemaDerivation.schemaForRecord(type);
        var components = type.getRecordComponents();
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < components.length; i++) fields.add(SchemaDerivation.withParameterDictionary(plain.getFields().get(i), components[i], i));
        return new Schema(fields, plain.getCustomMetadata());
    }

    private static boolean sameName(String a, String b) {
        return a.equals(b) || (a.equals("item") && b.equals("$data$")) || (a.equals("$data$") && b.equals("item"));
    }

    /** Null is the canonical encoding for an empty category. */
    public static <T extends ArrowSerializableRecord> byte[] encode(List<T> records, Class<T> type) {
        return encode(records, type, 1024);
    }

    /** Encode ordinary records using the RPC field conversions and one unified dictionary set. */
    public static <T extends ArrowSerializableRecord> byte[] encode(List<T> records, Class<T> type, int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("catalog batch size must be positive");
        if (records.isEmpty()) return null;
        Schema schema = schema(type);
        List<Map<String, Object>> rows = records.stream().map(RecordCodec::toRowMap).toList();
        // Canonical enum member names preserve the current SDK's singleton contract.
        if (type == farm.query.vgi.protocol.FunctionInfo.class || type == farm.query.vgi.protocol.MacroInfo.class) {
            for (var row : rows) for (var field : schema.getFields()) {
                if (field.getDictionary() != null && row.get(field.getName()) instanceof String text)
                    row.put(field.getName(), text.toUpperCase(java.util.Locale.ROOT));
            }
        }
        try (Marshalling.EncodedRow encoded = Marshalling.encodeRowsForWire(schema, rows, Allocators.root());
             ByteArrayOutputStream output = new ByteArrayOutputStream();
             IpcStreamWriter writer = new IpcStreamWriter(output)) {
            VectorSchemaRoot root = encoded.root();
            for (int offset = 0; offset < records.size(); offset += batchSize) {
                if (offset == 0 && records.size() <= batchSize) {
                    writer.writeBatch(root, null, encoded.provider());
                } else {
                    VectorSchemaRoot physical = new VectorSchemaRoot(root.getFieldVectors());
                    try (VectorSchemaRoot batch = physical.slice(offset, Math.min(batchSize, records.size() - offset))) {
                        writer.writeBatch(new VectorSchemaRoot(schema.getFields(), batch.getFieldVectors(), batch.getRowCount()), null, encoded.provider());
                    }
                }
            }
            writer.writeEos();
            return output.toByteArray();
        } catch (Exception e) { throw new IllegalStateException("catalog collection encode failed", e); }
    }

    /** Read every row and every record batch, including dictionary updates. */
    public static <T extends ArrowSerializableRecord> List<T> decode(byte[] bytes, Class<T> type) {
        if (bytes == null) return List.of();
        List<T> records = new ArrayList<>();
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        try (IpcStreamReader reader = new IpcStreamReader(input, Allocators.root())) {
            Schema expected = schema(type);
            if (!sameFields(reader.wireSchema().getFields(), expected.getFields())) {
                throw new IllegalArgumentException("catalog collection schema does not match " + type.getSimpleName());
            }
            Map<String,String> metadata;
            while ((metadata = reader.readNextBatch()) != null) {
                if(metadata.keySet().stream().anyMatch(key->key.startsWith("vgi_rpc.location")||key.startsWith("vgi_rpc.log_")||key.startsWith("vgi_rpc.error_"))) throw new IllegalArgumentException("Nested catalog metadata cannot contain external pointers");
                VectorSchemaRoot root = reader.root();
                for(int column=0;column<expected.getFields().size();column++)for(int row=0;row<root.getRowCount();row++) validateValue(root.getVector(column),expected.getFields().get(column),reader.wireSchema().getFields().get(column),row,reader.dictionaryProvider());
                for (int i = 0; i < root.getRowCount(); i++) {
                    try (VectorSchemaRoot row = root.slice(i, 1)) {
                        records.add(RecordCodec.fromRowMap(type, Marshalling.decodeRow(row, reader.dictionaryProvider(), reader.wireSchema())));
                    }
                }
            }
            if(input.available()!=0)throw new IllegalArgumentException("Trailing catalog metadata bytes");
            return records;
        } catch (Exception e) { throw new IllegalStateException("catalog collection decode failed for " + type.getSimpleName() + ": " + e.getMessage(), e); }
    }

    private static void validateValue(org.apache.arrow.vector.FieldVector vector, Field field, Field actual, int row, org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries) {
        if(vector.isNull(row)){if(!field.isNullable())throw new IllegalArgumentException("Required catalog field is null: "+field.getName());return;}
        if(field.getDictionary()!=null){
            int index=((Number)vector.getObject(row)).intValue();var values=dictionaries.lookup(actual.getDictionary().getId()).getVector();
            if(index<0||index>=values.getValueCount()||(values.isNull(index)&&!field.isNullable()))throw new IllegalArgumentException("Invalid catalog dictionary value");
        } else if(vector instanceof org.apache.arrow.vector.complex.StructVector struct){
            for(int i=0;i<field.getChildren().size();i++)validateValue(struct.getChildrenFromFields().get(i),field.getChildren().get(i),actual.getChildren().get(i),row,dictionaries);
        } else if(vector instanceof org.apache.arrow.vector.complex.ListVector list){
            for(int i=list.getElementStartIndex(row);i<list.getElementEndIndex(row);i++)validateValue(list.getDataVector(),field.getChildren().get(0),actual.getChildren().get(0),i,dictionaries);
        }
    }

    private static boolean sameFields(List<Field> actual, List<Field> expected) {
        if (actual.size() != expected.size()) return false;
        for (int i = 0; i < actual.size(); i++) {
            Field a = actual.get(i), b = expected.get(i);
            if (!sameName(a.getName(), b.getName()) || !a.getType().equals(b.getType())
                    || (a.getDictionary() == null) != (b.getDictionary() == null)
                    || (a.getDictionary() != null && !a.getDictionary().getIndexType().equals(b.getDictionary().getIndexType()))
                    || !sameFields(a.getChildren(), b.getChildren())) return false;
        }
        return true;
    }

    /** Adapter for author hooks returning independently encoded metadata items. */
    public static <T extends ArrowSerializableRecord> byte[] merge(List<byte[]> items, Class<T> type) {
        List<T> records = new ArrayList<>();
        for (byte[] item : items) records.addAll(decode(item, type));
        return encode(records, type);
    }
}
