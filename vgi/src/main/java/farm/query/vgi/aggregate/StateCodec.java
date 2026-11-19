// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.aggregate;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small, typed, bounds-checked binary codec for aggregate state
 * ({@link AggregateFunction#serializeState} / {@link AggregateFunction#deserializeState}).
 *
 * <p>The encoding is explicit: the reader gets back exactly the primitive
 * fields the writer wrote, in order, and no class is ever named or
 * instantiated from the bytes, so stored state cannot be turned into code
 * execution the way {@code ObjectInputStream} could. Every length is checked
 * against the bytes that remain, and {@link #decode} fails if bytes are left
 * over, so truncated or tampered state is a clean {@link IllegalArgumentException}.
 *
 * <pre>{@code
 * byte[] bytes = StateCodec.encode(out -> {
 *     out.writeLong(state.count);
 *     out.writeDouble(state.total);
 *     out.writeStringList(state.items);
 * });
 * State back = StateCodec.decode(bytes, in -> {
 *     State s = new State();
 *     s.count = in.readLong();
 *     s.total = in.readDouble();
 *     s.items = in.readStringList();
 *     return s;
 * });
 * }</pre>
 *
 * <p>Multi-byte numbers are big-endian; strings are UTF-8 with a 4-byte length.
 * {@link Output#writeValue} / {@link Input#readValue} carry a tagged scalar or
 * nested value from a fixed set of types (null, {@code Long}, {@code Integer},
 * {@code Short}, {@code Byte}, {@code Double}, {@code Float}, {@code Boolean},
 * {@code String}, {@code byte[]}, {@code BigDecimal}, {@code BigInteger},
 * {@code List}, {@code Object[]}, and {@code Map} with {@code String} keys).
 */
public final class StateCodec {

    private StateCodec() {}

    /** Writes a state's fields. */
    @FunctionalInterface
    public interface Writer {
        /**
         * Write the fields.
         *
         * @param out the sink
         */
        void write(Output out);
    }

    /**
     * Reads a state back.
     *
     * @param <S> the state type
     */
    @FunctionalInterface
    public interface Reader<S> {
        /**
         * Read the fields, in the order they were written.
         *
         * @param in the source
         * @return the state
         */
        S read(Input in);
    }

    /**
     * Encode with {@code writer}.
     *
     * @param writer writes the fields
     * @return the encoded bytes
     */
    public static byte[] encode(Writer writer) {
        Output out = new Output();
        writer.write(out);
        return out.toByteArray();
    }

    /**
     * Decode with {@code reader}, requiring every byte to be consumed.
     *
     * @param bytes the encoded bytes
     * @param reader reads the fields
     * @param <S> the state type
     * @return the state
     * @throws IllegalArgumentException if the bytes are truncated, malformed or have trailing data
     */
    public static <S> S decode(byte[] bytes, Reader<S> reader) {
        Input in = new Input(bytes);
        S s;
        try {
            s = reader.read(in);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("aggregate state is truncated", e);
        }
        if (in.remaining() != 0) {
            throw new IllegalArgumentException("aggregate state has " + in.remaining() + " trailing bytes");
        }
        return s;
    }

    // Value tags for writeValue / readValue.
    private static final byte T_NULL = 0;
    private static final byte T_LONG = 1;
    private static final byte T_INT = 2;
    private static final byte T_SHORT = 3;
    private static final byte T_BYTE = 4;
    private static final byte T_DOUBLE = 5;
    private static final byte T_FLOAT = 6;
    private static final byte T_BOOL = 7;
    private static final byte T_STRING = 8;
    private static final byte T_BYTES = 9;
    private static final byte T_DECIMAL = 10;
    private static final byte T_BIGINT = 11;
    private static final byte T_LIST = 12;
    private static final byte T_ARRAY = 13;
    private static final byte T_MAP = 14;
    private static final int MAX_DEPTH = 64;

    /** The write side. */
    public static final class Output {
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        private final ByteBuffer scratch = ByteBuffer.allocate(8);

        Output() {}

        byte[] toByteArray() {
            return buf.toByteArray();
        }

        private void putScratch(int n) {
            buf.write(scratch.array(), 0, n);
            scratch.clear();
        }

        /**
         * Write long.
         *
         * @param v the value
         */
        public void writeLong(long v) {
            scratch.putLong(0, v);
            putScratch(8);
        }

        /**
         * Write int.
         *
         * @param v the value
         */
        public void writeInt(int v) {
            scratch.putInt(0, v);
            putScratch(4);
        }

        /**
         * Write double.
         *
         * @param v the value
         */
        public void writeDouble(double v) {
            writeLong(Double.doubleToRawLongBits(v));
        }

        /**
         * Write boolean.
         *
         * @param v the value
         */
        public void writeBoolean(boolean v) {
            buf.write(v ? 1 : 0);
        }

        /**
         * Write bytes.
         *
         * @param v the value, or {@code null}
         */
        public void writeBytes(byte[] v) {
            if (v == null) {
                writeInt(-1);
                return;
            }
            writeInt(v.length);
            buf.write(v, 0, v.length);
        }

        /**
         * Write string.
         *
         * @param v the value, or {@code null}
         */
        public void writeString(String v) {
            writeBytes(v == null ? null : v.getBytes(StandardCharsets.UTF_8));
        }

        /**
         * Write string list.
         *
         * @param v the list, or {@code null}; its elements may be {@code null}
         */
        public void writeStringList(List<String> v) {
            if (v == null) {
                writeInt(-1);
                return;
            }
            writeInt(v.size());
            for (String s : v) writeString(s);
        }

        /**
         * Write double list.
         *
         * @param v the list, or {@code null}; its elements must not be {@code null}
         */
        public void writeDoubleList(List<Double> v) {
            if (v == null) {
                writeInt(-1);
                return;
            }
            writeInt(v.size());
            for (Double d : v) writeDouble(d);
        }

        /**
         * Write long list.
         *
         * @param v the list, or {@code null}; its elements must not be {@code null}
         */
        public void writeLongList(List<Long> v) {
            if (v == null) {
                writeInt(-1);
                return;
            }
            writeInt(v.size());
            for (Long d : v) writeLong(d);
        }

        /**
         * Write a tagged value from the supported set (see {@link StateCodec}).
         *
         * @param v the value
         * @throws IllegalArgumentException for an unsupported type
         */
        public void writeValue(Object v) {
            writeValue(v, 0);
        }

        private void writeValue(Object v, int depth) {
            if (depth > MAX_DEPTH) throw new IllegalArgumentException("aggregate state value nested too deeply");
            if (v == null) {
                buf.write(T_NULL);
            } else if (v instanceof Long l) {
                buf.write(T_LONG);
                writeLong(l);
            } else if (v instanceof Integer i) {
                buf.write(T_INT);
                writeInt(i);
            } else if (v instanceof Short sh) {
                buf.write(T_SHORT);
                writeInt(sh);
            } else if (v instanceof Byte b) {
                buf.write(T_BYTE);
                writeInt(b);
            } else if (v instanceof Double d) {
                buf.write(T_DOUBLE);
                writeDouble(d);
            } else if (v instanceof Float f) {
                buf.write(T_FLOAT);
                writeInt(Float.floatToRawIntBits(f));
            } else if (v instanceof Boolean b) {
                buf.write(T_BOOL);
                writeBoolean(b);
            } else if (v instanceof String s) {
                buf.write(T_STRING);
                writeString(s);
            } else if (v instanceof byte[] b) {
                buf.write(T_BYTES);
                writeBytes(b);
            } else if (v instanceof BigDecimal d) {
                buf.write(T_DECIMAL);
                writeInt(d.scale());
                writeBytes(d.unscaledValue().toByteArray());
            } else if (v instanceof BigInteger bi) {
                buf.write(T_BIGINT);
                writeBytes(bi.toByteArray());
            } else if (v instanceof List<?> list) {
                buf.write(T_LIST);
                writeInt(list.size());
                for (Object o : list) writeValue(o, depth + 1);
            } else if (v instanceof Object[] arr) {
                buf.write(T_ARRAY);
                writeInt(arr.length);
                for (Object o : arr) writeValue(o, depth + 1);
            } else if (v instanceof Map<?, ?> map) {
                buf.write(T_MAP);
                writeInt(map.size());
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    if (!(e.getKey() instanceof String k)) {
                        throw new IllegalArgumentException("aggregate state map keys must be strings");
                    }
                    writeString(k);
                    writeValue(e.getValue(), depth + 1);
                }
            } else {
                throw new IllegalArgumentException(
                        "unsupported aggregate state value type " + v.getClass().getName());
            }
        }
    }

    /** The read side; every length is checked against the remaining bytes. */
    public static final class Input {
        private final ByteBuffer buf;

        Input(byte[] bytes) {
            this.buf = ByteBuffer.wrap(bytes);
        }

        /**
         * Remaining.
         *
         * @return the number of unread bytes
         */
        public int remaining() {
            return buf.remaining();
        }

        /**
         * Read long.
         *
         * @return the value
         */
        public long readLong() {
            return buf.getLong();
        }

        /**
         * Read int.
         *
         * @return the value
         */
        public int readInt() {
            return buf.getInt();
        }

        /**
         * Read double.
         *
         * @return the value
         */
        public double readDouble() {
            return buf.getDouble();
        }

        /**
         * Read boolean.
         *
         * @return the value
         */
        public boolean readBoolean() {
            byte b = buf.get();
            if (b != 0 && b != 1) throw new IllegalArgumentException("aggregate state: bad boolean " + b);
            return b == 1;
        }

        /**
         * A count written before {@code minBytesEach}-byte elements, or {@code -1} for null.
         *
         * @param minBytesEach the smallest encoding of one element (bounds the count)
         * @return the count, or {@code -1}
         */
        private int readCount(int minBytesEach) {
            int n = buf.getInt();
            if (n == -1) return -1;
            if (n < 0 || (long) n * Math.max(1, minBytesEach) > buf.remaining()) {
                throw new IllegalArgumentException("aggregate state: bad length " + n);
            }
            return n;
        }

        /**
         * Read bytes.
         *
         * @return the value, or {@code null}
         */
        public byte[] readBytes() {
            int n = readCount(1);
            if (n < 0) return null;
            byte[] out = new byte[n];
            buf.get(out);
            return out;
        }

        /**
         * Read string.
         *
         * @return the value, or {@code null}
         */
        public String readString() {
            byte[] b = readBytes();
            return b == null ? null : new String(b, StandardCharsets.UTF_8);
        }

        /**
         * Read string list.
         *
         * @return a mutable list, or {@code null}
         */
        public ArrayList<String> readStringList() {
            int n = readCount(4);
            if (n < 0) return null;
            ArrayList<String> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) out.add(readString());
            return out;
        }

        /**
         * Read double list.
         *
         * @return a mutable list, or {@code null}
         */
        public ArrayList<Double> readDoubleList() {
            int n = readCount(8);
            if (n < 0) return null;
            ArrayList<Double> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) out.add(readDouble());
            return out;
        }

        /**
         * Read long list.
         *
         * @return a mutable list, or {@code null}
         */
        public ArrayList<Long> readLongList() {
            int n = readCount(8);
            if (n < 0) return null;
            ArrayList<Long> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) out.add(readLong());
            return out;
        }

        /**
         * Read a value written by {@link Output#writeValue}. Lists come back as
         * {@code ArrayList}, maps as {@code LinkedHashMap}, arrays as {@code Object[]}.
         *
         * @return the value
         */
        public Object readValue() {
            return readValue(0);
        }

        private Object readValue(int depth) {
            if (depth > MAX_DEPTH) throw new IllegalArgumentException("aggregate state value nested too deeply");
            byte tag = buf.get();
            switch (tag) {
                case T_NULL: return null;
                case T_LONG: return buf.getLong();
                case T_INT: return buf.getInt();
                case T_SHORT: return (short) buf.getInt();
                case T_BYTE: return (byte) buf.getInt();
                case T_DOUBLE: return buf.getDouble();
                case T_FLOAT: return Float.intBitsToFloat(buf.getInt());
                case T_BOOL: return readBoolean();
                case T_STRING: return requireNonNull(readString());
                case T_BYTES: return requireNonNull(readBytes());
                case T_DECIMAL: {
                    int scale = buf.getInt();
                    return new BigDecimal(new BigInteger(requireNonNull(readBytes())), scale);
                }
                case T_BIGINT: return new BigInteger(requireNonNull(readBytes()));
                case T_LIST: {
                    int n = readCount(1);
                    if (n < 0) throw new IllegalArgumentException("aggregate state: bad list length");
                    List<Object> out = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) out.add(readValue(depth + 1));
                    return out;
                }
                case T_ARRAY: {
                    int n = readCount(1);
                    if (n < 0) throw new IllegalArgumentException("aggregate state: bad array length");
                    Object[] out = new Object[n];
                    for (int i = 0; i < n; i++) out[i] = readValue(depth + 1);
                    return out;
                }
                case T_MAP: {
                    int n = readCount(5);
                    if (n < 0) throw new IllegalArgumentException("aggregate state: bad map length");
                    Map<String, Object> out = new LinkedHashMap<>();
                    for (int i = 0; i < n; i++) {
                        String k = requireNonNull(readString());
                        out.put(k, readValue(depth + 1));
                    }
                    return out;
                }
                default:
                    throw new IllegalArgumentException("aggregate state: unknown value tag " + tag);
            }
        }

        private static <T> T requireNonNull(T v) {
            if (v == null) throw new IllegalArgumentException("aggregate state: unexpected null");
            return v;
        }
    }
}
