// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.txfix;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * The txfix fixture's state, shared by every worker process: one file per {@code token}
 * partition under {@code $VGI_TXFIX_STATE_DIR} (default {@code <tmp>/vgi-txfix}), read and
 * rewritten under an exclusive file lock (plus an in-process lock, since a JVM cannot hold
 * two overlapping file locks).
 */
final class TxFixState {

    /** One {@code items} row. */
    record Row(int id, String name, Long qty) {
        Map<String, Object> asMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("name", name);
            m.put("qty", qty);
            return m;
        }
    }

    /** One event-log entry. */
    record Event(long seq, String kind, String txId, boolean ok) {}

    static final List<Row> INITIAL = List.of(new Row(1, "a", 10L), new Row(2, "b", 20L), new Row(3, "c", 30L));

    private static final ReentrantLock IN_PROCESS = new ReentrantLock();

    /** One token's state, loaded for the duration of {@link #withPartition}. */
    static final class Partition {
        final TreeMap<Integer, Row> committed = new TreeMap<>();
        final Map<String, TreeMap<Integer, Row>> staged = new LinkedHashMap<>();
        final List<Event> events = new ArrayList<>();
        long seq;

        void reset() {
            committed.clear();
            for (Row r : INITIAL) committed.put(r.id(), r);
            staged.clear();
            events.clear();
            seq = 0;
        }

        /** The rows a scan in {@code txId} sees: committed, overlaid with its staged changes. */
        TreeMap<Integer, Row> view(String txId) {
            TreeMap<Integer, Row> out = new TreeMap<>(committed);
            if (txId != null && staged.containsKey(txId)) out.putAll(staged.get(txId));
            return out;
        }

        void event(String kind, String txId, boolean ok) {
            events.add(new Event(++seq, kind, txId, ok));
        }
    }

    private TxFixState() {}

    static Path dir() {
        String d = System.getenv("VGI_TXFIX_STATE_DIR");
        return d != null && !d.isBlank() ? Path.of(d) : Path.of(System.getProperty("java.io.tmpdir"), "vgi-txfix");
    }

    private static String partitionName(String token) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    (token == null ? "\0null" : "t:" + token).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Run {@code body} on the token's partition under its lock, saving whatever it changed. */
    static <T> T withPartition(String token, Function<Partition, T> body) {
        Path dir = dir();
        String name = partitionName(token);
        IN_PROCESS.lock();
        try {
            Files.createDirectories(dir);
            try (FileChannel lockChannel = FileChannel.open(dir.resolve(name + ".lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 FileLock ignored = lockChannel.lock()) {
                Path file = dir.resolve(name + ".state");
                Partition p = new Partition();
                if (Files.exists(file)) load(p, Files.readAllLines(file, StandardCharsets.UTF_8));
                else p.reset();
                T result = body.apply(p);
                Path tmp = dir.resolve(name + ".state.tmp");
                Files.write(tmp, save(p), StandardCharsets.UTF_8);
                Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                return result;
            }
        } catch (IOException e) {
            throw new IllegalStateException("txfix: state I/O failed: " + e.getMessage(), e);
        } finally {
            IN_PROCESS.unlock();
        }
    }

    private static String enc(String s) {
        return s == null ? "\\N" : URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String dec(String s) {
        return "\\N".equals(s) ? null : URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static Long decLong(String s) {
        return "\\N".equals(s) ? null : Long.valueOf(s);
    }

    private static List<String> save(Partition p) {
        List<String> out = new ArrayList<>();
        out.add("seq\t" + p.seq);
        for (Row r : p.committed.values()) {
            out.add("row\t" + r.id() + "\t" + enc(r.name()) + "\t" + (r.qty() == null ? "\\N" : r.qty()));
        }
        for (Map.Entry<String, TreeMap<Integer, Row>> e : p.staged.entrySet()) {
            for (Row r : e.getValue().values()) {
                out.add("staged\t" + e.getKey() + "\t" + r.id() + "\t" + enc(r.name()) + "\t"
                        + (r.qty() == null ? "\\N" : r.qty()));
            }
        }
        for (Event ev : p.events) {
            out.add("event\t" + ev.seq() + "\t" + ev.kind() + "\t" + enc(ev.txId()) + "\t" + ev.ok());
        }
        return out;
    }

    private static void load(Partition p, List<String> lines) {
        for (String line : lines) {
            String[] f = line.split("\t", -1);
            switch (f[0]) {
                case "seq" -> p.seq = Long.parseLong(f[1]);
                case "row" -> p.committed.put(Integer.parseInt(f[1]),
                        new Row(Integer.parseInt(f[1]), dec(f[2]), decLong(f[3])));
                case "staged" -> p.staged.computeIfAbsent(f[1], k -> new TreeMap<>()).put(Integer.parseInt(f[2]),
                        new Row(Integer.parseInt(f[2]), dec(f[3]), decLong(f[4])));
                case "event" -> p.events.add(new Event(Long.parseLong(f[1]), f[2], dec(f[3]),
                        Boolean.parseBoolean(f[4])));
                default -> { }
            }
        }
    }
}
