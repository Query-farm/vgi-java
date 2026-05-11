// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.PortableStreamState;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

/**
 * Bridges VGI's scalar function abstraction onto vgi-rpc-java's exchange-stream
 * lifecycle. Every field is wire-portable (String + byte[]); the function
 * itself is looked up on each exchange via {@link ServiceLocator} so the
 * state can round-trip across HTTP workers.
 */
public final class ScalarStreamState extends ExchangeState implements PortableStreamState {

    private String functionName;
    private int argCount;
    private int variantIndex;
    private byte[] outputSchemaIpc;
    private byte[] argumentsIpc;
    private byte[] settingsIpc;
    private byte[] secrets;

    private transient Schema cachedOutputSchema;
    private transient Arguments cachedArguments;
    private transient Map<String, Object> cachedSettings;

    /** No-arg ctor for {@link PortableStreamState} round-trip. */
    public ScalarStreamState() {}

    public ScalarStreamState(String functionName, int argCount, byte[] outputSchemaIpc,
                              byte[] argumentsIpc, byte[] settingsIpc) {
        this(functionName, argCount, 0, outputSchemaIpc, argumentsIpc, settingsIpc);
    }

    public ScalarStreamState(String functionName, int argCount, int variantIndex,
                              byte[] outputSchemaIpc, byte[] argumentsIpc, byte[] settingsIpc) {
        this.functionName = functionName;
        this.argCount = argCount;
        this.variantIndex = variantIndex;
        this.outputSchemaIpc = outputSchemaIpc;
        this.argumentsIpc = argumentsIpc;
        this.settingsIpc = settingsIpc;
    }

    public void setSecrets(byte[] secrets) { this.secrets = secrets; }

    @Override
    public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        ScalarFunction fn = ServiceLocator.current().scalarAt(functionName, variantIndex);
        if (cachedOutputSchema == null) cachedOutputSchema = SchemaUtil.deserializeSchema(outputSchemaIpc);
        if (cachedArguments == null) cachedArguments = ArgumentsParser.parse(argumentsIpc);
        if (cachedSettings == null) cachedSettings = SettingsParser.parse(settingsIpc);
        ScalarProcessParams params = new ScalarProcessParams(
                functionName, cachedArguments, cachedOutputSchema, cachedSettings, secrets);
        VectorSchemaRoot result = fn.process(params, input.root(), Allocators.root());
        out.emit(result);
    }

    @Override
    public byte[] encode() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(functionName);
            dos.writeInt(argCount);
            dos.writeInt(variantIndex);
            writeBytes(dos, outputSchemaIpc);
            writeBytes(dos, argumentsIpc);
            writeBytes(dos, settingsIpc);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("ScalarStreamState.encode", e);
        }
    }

    @Override
    public void decode(byte[] data) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            this.functionName = dis.readUTF();
            this.argCount = dis.readInt();
            this.variantIndex = dis.readInt();
            this.outputSchemaIpc = readBytes(dis);
            this.argumentsIpc = readBytes(dis);
            this.settingsIpc = readBytes(dis);
        } catch (Exception e) {
            throw new RuntimeException("ScalarStreamState.decode", e);
        }
    }

    static void writeBytes(DataOutputStream dos, byte[] b) throws java.io.IOException {
        if (b == null) { dos.writeInt(-1); return; }
        dos.writeInt(b.length);
        dos.write(b);
    }

    static byte[] readBytes(DataInputStream dis) throws java.io.IOException {
        int n = dis.readInt();
        if (n < 0) return null;
        byte[] out = new byte[n];
        dis.readFully(out);
        return out;
    }
}
