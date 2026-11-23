// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.PortableStreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.stateref.StateRef;
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
 * lifecycle. The init pieces (output schema, arguments, settings, secrets) ride as one
 * {@code init: StateRef} (protocol 3.4), never as separate cursor fields; the function
 * itself is looked up on each exchange via {@link ServiceLocator} so the
 * state can round-trip across HTTP workers.
 */
public final class ScalarStreamState extends ExchangeState implements PortableStreamState {

    private static final int FORM_STATE_REF = 1;
    private static final int FORM_INLINE_RECORD = 2;

    private String functionName;
    private int argCount;
    private int variantIndex;
    /** The init pieces (protocol 3.4 {@code init: StateRef}); created in the init handler only. */
    private StateRef init;
    /** The same pieces when the init context offered no StateRef (direct calls, tests). */
    private byte[] inlineInit;

    private transient ScalarStreamInit cachedInit;
    private transient Schema cachedOutputSchema;
    private transient Arguments cachedArguments;
    private transient Map<String, Object> cachedSettings;

    /** No-arg ctor for {@link PortableStreamState} round-trip. */
    public ScalarStreamState() {}

    /**
     * Capture the bind context a scalar needs on each exchange, as one {@code init} StateRef.
     *
     * <p>Called from the init handler: the state ref is created on that turn's context and
     * every later turn copies it. When the context offers none (no transport), the record is
     * carried inline instead.
     *
     * @param functionName registered scalar name, used to relocate the function via {@link ServiceLocator}
     * @param argCount declared argument count (positional consts + input columns)
     * @param variantIndex which overload of {@code functionName} this bound to
     * @param init the init pieces
     * @param resolvedArguments the already-parsed arguments (kept in memory for this process)
     * @param ctx the init handler's context, or {@code null}
     */
    public ScalarStreamState(String functionName, int argCount, int variantIndex,
                              ScalarStreamInit init, Arguments resolvedArguments, CallContext ctx) {
        this.functionName = functionName;
        this.argCount = argCount;
        this.variantIndex = variantIndex;
        if (ctx != null && ctx.stateRefScope().isInit()) {
            this.init = ctx.stateRef(init);
        } else {
            this.inlineInit = RecordCodec.serializeToBytes(init);
        }
        this.cachedInit = init;
        this.cachedArguments = resolvedArguments;
    }

    /**
     * Run the bound scalar over one input batch and emit its result. Lazily
     * relocates the function and resolves the init (and an argument reference
     * through the object cache), so the call works after a state round-trip over HTTP.
     *
     * @param input the input batch (its columns are the scalar's per-row inputs)
     * @param out collector that takes ownership of the emitted result root
     * @param ctx the per-call RPC context
     */
    @Override
    public void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        ScalarFunction fn = ServiceLocator.current().scalarAt(functionName, variantIndex);
        ScalarStreamInit pieces = initPieces(ctx);
        if (cachedOutputSchema == null) cachedOutputSchema = SchemaUtil.deserializeSchema(pieces.output_schema());
        if (cachedArguments == null) {
            cachedArguments = pieces.arguments_location() != null
                    ? BindArguments.parse(BindArguments.resolveLocation(pieces.arguments_location(), ctx), true)
                    : ArgumentsParser.parse(pieces.arguments());
        }
        if (cachedSettings == null) cachedSettings = SettingsParser.parse(pieces.settings());
        ScalarProcessParams params = new ScalarProcessParams(
                functionName, cachedArguments, cachedOutputSchema, cachedSettings, pieces.secrets());
        VectorSchemaRoot result = fn.process(params, input.root(), Allocators.root());
        // Result-cache opt-in: a scalar declaring cacheControl() rides its
        // vgi.cache.* keys on the emit path's per-batch custom_metadata, so the
        // extension can memoize the output per distinct input value.
        farm.query.vgi.cache.CacheControl cc = fn.cacheControl();
        out.emit(result, cc == null ? null : cc.toMetadata());
    }

    private ScalarStreamInit initPieces(CallContext ctx) {
        ScalarStreamInit pieces = cachedInit;
        if (pieces == null) {
            if (init != null) {
                if (ctx == null) throw new IllegalStateException("scalar stream: init StateRef needs a context");
                pieces = ctx.resolve(init, ScalarStreamInit.class);
            } else if (inlineInit != null) {
                pieces = RecordCodec.deserializeFromBytes(inlineInit, ScalarStreamInit.class);
            } else {
                throw new IllegalStateException("scalar stream state carries no init");
            }
            cachedInit = pieces;
        }
        return pieces;
    }

    /**
     * Serialise this state to a portable token (used as the HTTP {@code /exchange} continuation).
     * The init rides as its StateRef's encoded bytes (inline or a stored tag), deciding its form
     * on the first serialization inside a cursor mint.
     *
     * @return the encoded byte token
     */
    @Override
    public byte[] encode() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(functionName);
            dos.writeInt(argCount);
            dos.writeInt(variantIndex);
            if (init != null) {
                dos.writeByte(FORM_STATE_REF);
                writeBytes(dos, init.toBytes());
            } else {
                dos.writeByte(FORM_INLINE_RECORD);
                writeBytes(dos, inlineInit);
            }
            return baos.toByteArray();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("ScalarStreamState.encode", e);
        }
    }

    /**
     * Rehydrate this state from a token produced by {@link #encode}.
     *
     * @param data the encoded byte token
     */
    @Override
    public void decode(byte[] data) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            this.functionName = dis.readUTF();
            this.argCount = dis.readInt();
            this.variantIndex = dis.readInt();
            int form = dis.readUnsignedByte();
            byte[] body = readBytes(dis);
            if (form == FORM_STATE_REF && body != null) {
                this.init = StateRef.fromBytes(body);
            } else if (form == FORM_INLINE_RECORD && body != null) {
                this.inlineInit = body;
            } else {
                throw new IllegalArgumentException("unknown scalar stream init form " + form);
            }
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
