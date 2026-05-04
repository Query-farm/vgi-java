// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.PortableStreamState;
import farm.query.vgirpc.ProducerState;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * Drives the FINALIZE phase of a table-in-out function by emitting a
 * pre-built list of buffered batches one per producer tick.
 *
 * <p>State is encoded as a sequence of self-contained Arrow IPC streams (one
 * per batch) plus an int cursor — wire-portable across HTTP workers via
 * {@link PortableStreamState}.</p>
 */
public final class FinalizeProducerState extends ProducerState implements PortableStreamState {

    private List<byte[]> batchesIpc = new ArrayList<>();
    private int cursor;

    public FinalizeProducerState() {}

    public FinalizeProducerState(List<VectorSchemaRoot> batches) {
        for (VectorSchemaRoot root : batches) {
            try (root) {
                batchesIpc.add(toIpc(root));
            }
        }
    }

    @Override
    public void produce(OutputCollector out, CallContext ctx) {
        if (cursor >= batchesIpc.size()) {
            out.finish();
            return;
        }
        byte[] data = batchesIpc.get(cursor++);
        out.emit(fromIpc(data));
    }

    @Override
    public byte[] encode() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(cursor);
            dos.writeInt(batchesIpc.size());
            for (byte[] b : batchesIpc) {
                dos.writeInt(b.length);
                dos.write(b);
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("FinalizeProducerState.encode", e);
        }
    }

    @Override
    public void decode(byte[] data) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            this.cursor = dis.readInt();
            int n = dis.readInt();
            this.batchesIpc = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int len = dis.readInt();
                byte[] b = new byte[len];
                dis.readFully(b);
                this.batchesIpc.add(b);
            }
        } catch (Exception e) {
            throw new RuntimeException("FinalizeProducerState.decode", e);
        }
    }

    private static byte[] toIpc(VectorSchemaRoot root) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ArrowStreamWriter w = new ArrowStreamWriter(root,
                     new DictionaryProvider.MapDictionaryProvider(),
                     Channels.newChannel(baos))) {
            w.start();
            w.writeBatch();
            w.end();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("FinalizeProducerState.toIpc", e);
        }
    }

    private static VectorSchemaRoot fromIpc(byte[] data) {
        try {
            ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(data),
                    Allocators.root());
            reader.loadNextBatch();
            // Caller (OutputCollector.emit) is responsible for closing the root;
            // the reader instance can outlive this method since the root holds
            // its own memory once loaded — but to avoid leaking the reader
            // wrapper we copy into a fresh root and close the reader here.
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            VectorSchemaRoot copy = VectorSchemaRoot.create(src.getSchema(), Allocators.root());
            copy.allocateNew();
            int rows = src.getRowCount();
            for (int c = 0; c < copy.getFieldVectors().size(); c++) {
                for (int i = 0; i < rows; i++) {
                    copy.getVector(c).copyFromSafe(i, i, src.getVector(c));
                }
            }
            copy.setRowCount(rows);
            reader.close();
            return copy;
        } catch (Exception e) {
            throw new RuntimeException("FinalizeProducerState.fromIpc", e);
        }
    }
}
