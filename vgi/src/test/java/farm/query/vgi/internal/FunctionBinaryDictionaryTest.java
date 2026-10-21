package farm.query.vgi.internal;

import farm.query.vgi.MetadataCollections;
import farm.query.vgi.protocol.*;
import farm.query.vgirpc.marshal.*;
import farm.query.vgirpc.wire.*;
import org.apache.arrow.vector.VarBinaryVector;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.io.*;
import static org.junit.jupiter.api.Assertions.*;

public class FunctionBinaryDictionaryTest {
    public static List<FunctionInfo> rows() {
        var rows = new ArrayList<FunctionInfo>();
        for(int i=0;i<7;i++) {
            var values = new HashMap<String,Object>();
            for(var field:FunctionInfo.class.getRecordComponents()) {
                Class<?> type=field.getType();
                values.put(field.getName(), type==boolean.class ? false : type==List.class ? List.of() : type==Map.class ? Map.of() : null);
            }
            values.put("name", "f"+i); values.put("schema_path",List.of(i%2==0?"main":"extra"));
            values.put("argument_monotonicity",null); values.put("function_type","scalar"); values.put("description","");
            values.put("partition_kind","NOT_PARTITIONED"); values.put("order_dependent","NOT_ORDER_DEPENDENT"); values.put("distinct_dependent","NOT_DISTINCT_DEPENDENT");
            values.put("arguments",new byte[]{(byte)255,0,(byte)(i<3?128:129)});
            values.put("output_schema",new byte[0]);
            values.put("parameter_default_values",i%3==0?null:i%3==1?new byte[0]:new byte[]{(byte)255,0});
            rows.add(RecordCodec.fromRowMap(FunctionInfo.class, values));
        }
        return rows;
    }
    private static void same(List<FunctionInfo> expected,List<FunctionInfo> actual) {
        assertEquals(expected.size(),actual.size());
        for(int i=0;i<expected.size();i++) {
            assertArrayEquals(expected.get(i).arguments(),actual.get(i).arguments());
            assertArrayEquals(expected.get(i).output_schema(),actual.get(i).output_schema());
            assertArrayEquals(expected.get(i).parameter_default_values(),actual.get(i).parameter_default_values());
            assertEquals(expected.get(i).schema_path(),actual.get(i).schema_path());
        }
    }
    @Test void singletonAndCollectionsKeepBinaryDictionariesByContent() throws Exception {
        var rows=rows();
        same(List.of(rows.getFirst()),MetadataCollections.decode(FunctionInfoSerializer.serialize(rows.getFirst()),FunctionInfo.class));
        same(List.of(rows.getFirst()),List.of(RecordCodec.deserializeFromBytes(RecordCodec.serializeToBytes(rows.getFirst()),FunctionInfo.class)));
        byte[] bytes=MetadataCollections.encode(rows,FunctionInfo.class,3);
        try(var reader=new IpcStreamReader(new ByteArrayInputStream(bytes),Allocators.root())) {
            reader.readNextBatch();
            var field=reader.wireSchema().findField("arguments");
            assertEquals(32,field.getDictionary().getIndexType().getBitWidth());
            var dictionary=reader.dictionaryProvider().lookup(field.getDictionary().getId()).getVector();
            assertInstanceOf(VarBinaryVector.class,dictionary); assertEquals(2,dictionary.getValueCount());
        }
        same(rows,MetadataCollections.decode(bytes,FunctionInfo.class));
    }
    @Test void nullDictionaryValuesStayNullForOptionalDefaultsAndRejectRequiredArguments() throws Exception {
        for(String name:List.of("parameter_default_values","arguments")) {
            var row=rows().get(1); var schema=MetadataCollections.schema(FunctionInfo.class); var output=new ByteArrayOutputStream();
            try(var encoded=Marshalling.encodeRowForWire(schema,RecordCodec.toRowMap(row),Allocators.root());var writer=new IpcStreamWriter(output)) {
                var dictionary=encoded.provider().lookup(schema.findField(name).getDictionary().getId()).getVector();
                dictionary.setNull(0);
                writer.writeBatch(encoded.root(),null,encoded.provider());
            }
            if(name.equals("arguments")) assertThrows(IllegalStateException.class,()->MetadataCollections.decode(output.toByteArray(),FunctionInfo.class));
            else assertNull(MetadataCollections.decode(output.toByteArray(),FunctionInfo.class).getFirst().parameter_default_values());
        }
    }

    @Test void binaryDictionaryReplacementsAcrossBatchesRemainReadable() throws Exception {
        var rows=rows(); var output=new ByteArrayOutputStream();
        try(var writer=new IpcStreamWriter(output)) {
            for(var row:rows) try(var encoded=Marshalling.encodeRowForWire(MetadataCollections.schema(FunctionInfo.class),RecordCodec.toRowMap(row),Allocators.root())) {
                writer.writeBatch(encoded.root(),null,encoded.provider());
            }
        }
        same(rows,MetadataCollections.decode(output.toByteArray(),FunctionInfo.class));
    }
}
