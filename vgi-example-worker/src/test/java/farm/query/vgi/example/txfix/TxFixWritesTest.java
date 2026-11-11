// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.txfix;

import farm.query.vgi.write.WriteResults;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The txfix write contract (vgi {@code docs/writable_tx_fixtures.md}) at the unit level: the
 * insert / delete validation rules, staged deletes on the transactional attach, and the
 * {@code writes()} log surviving the state file.
 */
final class TxFixWritesTest {

    private static TreeMap<Integer, TxFixState.Row> initial() {
        TreeMap<Integer, TxFixState.Row> rows = new TreeMap<>();
        for (TxFixState.Row r : TxFixState.INITIAL) rows.put(r.id(), r);
        return rows;
    }

    /** An INSERT input batch: id, name, qty (a null array entry is SQL NULL). */
    private static VectorSchemaRoot insertBatch(Integer[] ids, String[] names, Long[] qtys) {
        VectorSchemaRoot root = VectorSchemaRoot.create(TxFixWorker.ITEMS_USER, Allocators.root());
        root.allocateNew();
        IntVector id = (IntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector qty = (BigIntVector) root.getVector("qty");
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] == null) id.setNull(i); else id.setSafe(i, ids[i]);
            if (names[i] == null) name.setNull(i); else name.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
            if (qtys[i] == null) qty.setNull(i); else qty.setSafe(i, qtys[i]);
        }
        root.setRowCount(ids.length);
        return root;
    }

    /** A DELETE input batch: rowid only. */
    private static VectorSchemaRoot deleteBatch(long... rowids) {
        Schema schema = new Schema(List.<Field>of(WriteResults.rowIdField()));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector(0);
        for (int i = 0; i < rowids.length; i++) v.setSafe(i, rowids[i]);
        root.setRowCount(rowids.length);
        return root;
    }

    private static TxFixWorker.InsertState insert(boolean transactional) {
        TxFixWorker.InsertState s = new TxFixWorker.InsertState();
        s.transactional = transactional;
        return s;
    }

    @Test
    void insertValidatesTheWholeBatch() {
        try (VectorSchemaRoot ok = insertBatch(new Integer[] {4, 5}, new String[] {"d", "e"}, new Long[] {0L, null})) {
            var changes = insert(false).plan(ok, initial());
            assertEquals(2, changes.size());
            assertNull(changes.get(0).before());
            assertEquals(new TxFixState.Row(4, "d", 0L), changes.get(0).after());
            assertEquals(new TxFixState.Row(5, "e", null), changes.get(1).after());
        }
        try (VectorSchemaRoot dup = insertBatch(new Integer[] {2}, new String[] {"x"}, new Long[] {1L})) {
            var e = assertThrows(IllegalArgumentException.class, () -> insert(false).plan(dup, initial()));
            assertEquals("txfix: duplicate id 2", e.getMessage());
        }
        try (VectorSchemaRoot dupInBatch = insertBatch(new Integer[] {7, 7}, new String[] {"x", "y"}, new Long[] {1L, 1L})) {
            var e = assertThrows(IllegalArgumentException.class, () -> insert(false).plan(dupInBatch, initial()));
            assertEquals("txfix: duplicate id 7", e.getMessage());
        }
        try (VectorSchemaRoot noName = insertBatch(new Integer[] {8}, new String[] {null}, new Long[] {1L})) {
            var e = assertThrows(IllegalArgumentException.class, () -> insert(false).plan(noName, initial()));
            assertEquals("txfix: name required", e.getMessage());
        }
        // qty < 0: refused at write time on the plain attach, accepted (refused at COMMIT) on ttx.
        try (VectorSchemaRoot negative = insertBatch(new Integer[] {9}, new String[] {"n"}, new Long[] {-1L})) {
            var e = assertThrows(IllegalArgumentException.class, () -> insert(false).plan(negative, initial()));
            assertEquals("txfix: qty must be >= 0", e.getMessage());
            assertEquals(1, insert(true).plan(negative, initial()).size());
        }
    }

    @Test
    void deleteCountsEachExistingRowOnce() {
        TxFixWorker.DeleteState s = new TxFixWorker.DeleteState();
        try (VectorSchemaRoot batch = deleteBatch(2, 2, 99, 3)) {
            var changes = s.plan(batch, initial());
            assertEquals(List.of(2, 3), changes.stream().map(TxFixWorker.WriteState.Change::id).toList());
            assertEquals(new TxFixState.Row(2, "b", 20L), changes.get(0).before());
            assertNull(changes.get(0).after());
        }
    }

    @Test
    void stagedInsertsAndDeletesOverlayAndApply() {
        TxFixState.Partition p = new TxFixState.Partition();
        p.reset();
        p.put("tx", 4, new TxFixState.Row(4, "d", 1L));
        p.put("tx", 1, null);
        assertEquals(List.of(2, 3, 4), List.copyOf(p.view("tx").keySet()));
        assertEquals(List.of(1, 2, 3), List.copyOf(p.view(null).keySet()));
        // Deleting a staged insert edits the staged state.
        p.put("tx", 4, null);
        assertEquals(List.of(2, 3), List.copyOf(p.view("tx").keySet()));
        p.apply(p.staged.remove("tx"));
        assertEquals(List.of(2, 3), List.copyOf(p.committed.keySet()));
        // The plain attach writes the committed rows directly.
        p.put(null, 5, new TxFixState.Row(5, "e", 0L));
        p.put(null, 2, null);
        assertEquals(List.of(3, 5), List.copyOf(p.committed.keySet()));
    }

    @Test
    void stateFileKeepsTombstonesAndTheWriteLog() {
        String partition = "unit-" + UUID.randomUUID();
        TxFixState.withPartition(partition, p -> {
            p.reset();
            p.put("abcd", 1, null);
            p.put("abcd", 4, new TxFixState.Row(4, "d", null));
            p.writeStarted("insert", null, "c1");
            p.wrote("c1", 3);
            p.writeStarted("delete", "abcd", "c2");
            p.finalized("c1");
            return null;
        });
        TxFixState.withPartition(partition, p -> {
            assertEquals(List.of(2, 3, 4), List.copyOf(p.view("abcd").keySet()));
            assertEquals(2, p.writes.size());
            TxFixState.Write first = p.writes.get(0);
            assertEquals(1, first.seq());
            assertEquals("insert", first.op());
            assertNull(first.txId());
            assertEquals(3, first.rows());
            assertTrue(first.finalized());
            TxFixState.Write second = p.writes.get(1);
            assertEquals(2, second.seq());
            assertEquals("abcd", second.txId());
            assertFalse(second.finalized());
            // reset() clears the write log and its sequence.
            p.reset();
            assertTrue(p.writes.isEmpty());
            p.writeStarted("update", null, "c3");
            assertEquals(1, p.writes.get(0).seq());
            return null;
        });
    }
}
