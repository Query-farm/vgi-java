// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.aggregate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StateCodecTest {

    @Test
    void primitivesAndListsRoundTrip() {
        byte[] b = StateCodec.encode(out -> {
            out.writeLong(-7);
            out.writeInt(3);
            out.writeDouble(2.5);
            out.writeBoolean(true);
            out.writeString("héllo");
            out.writeString(null);
            out.writeBytes(new byte[] {1, 2});
            out.writeStringList(List.of("a", "b"));
            out.writeStringList(null);
            out.writeDoubleList(List.of(1.0, 2.0));
            out.writeLongList(List.of(9L));
        });
        StateCodec.decode(b, in -> {
            assertEquals(-7, in.readLong());
            assertEquals(3, in.readInt());
            assertEquals(2.5, in.readDouble());
            assertEquals(true, in.readBoolean());
            assertEquals("héllo", in.readString());
            assertNull(in.readString());
            assertArrayEquals(new byte[] {1, 2}, in.readBytes());
            assertEquals(List.of("a", "b"), in.readStringList());
            assertNull(in.readStringList());
            assertEquals(List.of(1.0, 2.0), in.readDoubleList());
            assertEquals(List.of(9L), in.readLongList());
            return null;
        });
    }

    @Test
    void taggedValuesRoundTrip() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("x", 1L);
        m.put("y", null);
        List<Object> values = new ArrayList<>(List.of(1L, 2, 3.5, 1.5f, false, "s",
                new BigDecimal("12.345"), new BigInteger("123456789012345678901234567890"), m));
        values.add(null);
        byte[] b = StateCodec.encode(out -> {
            out.writeValue(values);
            out.writeValue(new Object[] {"a", 2L});
            out.writeValue(new byte[] {7});
        });
        StateCodec.decode(b, in -> {
            assertEquals(values, in.readValue());
            assertArrayEquals(new Object[] {"a", 2L}, (Object[]) in.readValue());
            assertArrayEquals(new byte[] {7}, (byte[]) in.readValue());
            return null;
        });
    }

    @Test
    void unsupportedTypeRefused() {
        assertThrows(IllegalArgumentException.class, () -> StateCodec.encode(out -> out.writeValue(new Object())));
    }

    @Test
    void truncatedTrailingAndBogusLengthsRefused() {
        byte[] b = StateCodec.encode(out -> out.writeLong(1));
        assertThrows(IllegalArgumentException.class,
                () -> StateCodec.decode(java.util.Arrays.copyOf(b, 4), in -> in.readLong()));
        assertThrows(IllegalArgumentException.class,
                () -> StateCodec.decode(java.util.Arrays.copyOf(b, 12), in -> in.readLong()));
        // A huge declared length must fail before allocating.
        byte[] bogus = StateCodec.encode(out -> out.writeInt(Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> StateCodec.decode(bogus, in -> in.readBytes()));
        assertThrows(IllegalArgumentException.class, () -> StateCodec.decode(bogus, in -> in.readStringList()));
        assertThrows(IllegalArgumentException.class,
                () -> StateCodec.decode(new byte[] {99}, in -> in.readValue()));
    }
}
