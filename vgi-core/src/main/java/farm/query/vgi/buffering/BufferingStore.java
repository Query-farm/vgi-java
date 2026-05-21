// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.buffering;

import farm.query.vgi.internal.HexId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process append-log store for table-buffering executions, keyed by
 * {@code execution_id}. Mirrors the {@code state_append}/{@code state_log_scan}
 * surface of vgi-python's {@code BoundStorage}, but lives entirely in the
 * worker process — sufficient for the integration suite, which runs a single
 * long-lived launcher worker (the worker behind the socket <em>is</em> the
 * pool, so Sink and Source share this process).
 *
 * <p>Keys are {@code (execution_id, namespace, key)}; each holds an ordered
 * append log. {@code id} is the 0-based position, used as a resumable cursor.</p>
 */
public final class BufferingStore {

    /** One appended record: its log position and value bytes. */
    public record Entry(long id, byte[] value) {}

    private final Map<String, List<byte[]>> logs = new ConcurrentHashMap<>();

    private static String key(byte[] exec, byte[] ns, byte[] k) {
        return HexId.encode(exec) + "|" + HexId.encode(ns) + "|" + HexId.encode(k);
    }

    /** Append {@code value}; return its log position (a resumable cursor id). */
    public synchronized long append(byte[] exec, byte[] ns, byte[] k, byte[] value) {
        List<byte[]> log = logs.computeIfAbsent(key(exec, ns, k), x -> new ArrayList<>());
        log.add(value);
        return log.size() - 1L;
    }

    /** Scan up to {@code limit} entries with {@code id > afterId}, in order. */
    public synchronized List<Entry> scan(byte[] exec, byte[] ns, byte[] k, long afterId, int limit) {
        List<byte[]> log = logs.get(key(exec, ns, k));
        List<Entry> out = new ArrayList<>();
        if (log == null) return out;
        for (long i = afterId + 1; i < log.size() && out.size() < limit; i++) {
            out.add(new Entry(i, log.get((int) i)));
        }
        return out;
    }

    /** Drop every log belonging to {@code exec} (best-effort end-of-query cleanup). */
    public synchronized void removeExecution(byte[] exec) {
        String prefix = HexId.encode(exec) + "|";
        logs.keySet().removeIf(k -> k.startsWith(prefix));
    }
}
