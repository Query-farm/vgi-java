// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.TypeRules;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Pythonic scalar function base. Subclass and define a single {@code compute()}
 * method; the framework reads its signature to derive {@link FunctionSpec},
 * output schema, and per-batch dispatch.
 *
 * <h2>compute() signature rules</h2>
 * <ul>
 *   <li>Parameters annotated {@code @Vector} are per-row inputs. The parameter's
 *       Java type (concrete Arrow vector class) drives Arrow type inference.
 *       Use {@code @Vector(any=true) FieldVector} for any-typed inputs and
 *       {@code @Vector(varargs=true) List<FieldVector>} for varargs.</li>
 *   <li>Parameters annotated {@code @Const} are bind-time constant positional args.
 *       Type mapping: {@code long/int → INT64}, {@code double → FLOAT64},
 *       {@code String → UTF8}, {@code boolean → BOOL}.</li>
 *   <li>Parameters annotated {@code @Setting} pull from session settings.</li>
 *   <li>{@code @OutputLength int} — injected batch row count.</li>
 *   <li>The <em>last</em> parameter that is an Arrow vector class (unannotated)
 *       is the <strong>output</strong> — pre-allocated by the framework with
 *       the right row count. {@code compute()} returns {@code void}.</li>
 * </ul>
 *
 * <p>For dynamic output types override {@link #outputType(Schema, Arguments)}.
 */
public abstract class ScalarFn implements ScalarFunction {

    private final ComputePlan plan = ComputePlan.forClass(getClass());

    /**
     * Sole constructor. Parses the subclass's {@code compute()} signature into
     * the cached dispatch plan, so an invalid signature (no output vector
     * parameter, unannotated inputs) fails here — at registration — rather
     * than at first call.
     */
    protected ScalarFn() {}

    /**
     * SQL function name.
     *
     * @return the SQL name this function registers under.
     */
    public abstract String name();

    /**
     * One-line description (shown by {@code duckdb_functions()}).
     *
     * @return the description; empty by default.
     */
    public String description() { return ""; }

    /**
     * Override for richer metadata (categories, pushdown, etc.).
     *
     * @return the function metadata; defaults to a description-only record.
     */
    public FunctionMetadata metadata() { return FunctionMetadata.describe(description()); }

    /**
     * Override when output type depends on input column types or const args.
     * Default returns the static type inferred from {@code compute()}'s output
     * vector parameter.
     *
     * @param inputSchema schema of the bound input columns, or {@code null}.
     * @param arguments resolved const arguments.
     * @return the Arrow type of the single output column.
     */
    protected ArrowType outputType(Schema inputSchema, Arguments arguments) {
        return plan.staticOutputType;
    }

    /**
     * Override for non-flat output types (STRUCT, LIST, FixedSizeList) whose
     * children need explicit declaration. Default builds a single-column
     * schema {@code "result": outputType()} with no children.
     *
     * @param inputSchema schema of the bound input columns, or {@code null}.
     * @param arguments resolved const arguments.
     * @return the full output schema (must have its first column named {@code "result"}).
     */
    protected Schema outputSchema(Schema inputSchema, Arguments arguments) {
        return Schemas.of(Schemas.nullable("result", outputType(inputSchema, arguments)));
    }

    /**
     * {@inheritDoc}
     *
     * @return the function spec built from {@link #name()}, {@link #metadata()}, and {@link #argumentSpecs()}.
     */
    @Override
    public final FunctionSpec spec() {
        return new FunctionSpec(name(), metadata(), argumentSpecs());
    }

    /**
     * Argument specs. Default: auto-derived from {@code compute()} parameter
     * annotations + Arrow vector class type-mapping. Override when arguments
     * use nested Arrow types ({@code STRUCT}, {@code LIST}, {@code FixedSizeList})
     * whose children can't be inferred from the Java vector class alone.
     * {@code compute()} still drives the per-batch dispatch — only argument
     * metadata changes.
     *
     * @return the argument specs, auto-derived from {@code compute()} by default.
     */
    @Override
    public List<ArgSpec> argumentSpecs() {
        return plan.argSpecs;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Resolves {@link #outputSchema} first (so fixtures with domain-specific
     * reject messages report before the framework's generic type-bound check),
     * then enforces any declared {@link TypeBoundPredicate}s.
     *
     * @param params the bind-time arguments, input schema, and settings.
     * @return the bind response carrying the serialized output schema.
     */
    @Override
    public final BindResponse onBind(ScalarBindParams params) {
        // outputSchema first so fixtures with custom-worded rejects (e.g. DoubleFunction)
        // produce their domain-specific error before the framework's generic one.
        Schema out = outputSchema(params.inputSchema(), params.arguments());
        enforceTypeBounds(params.inputSchema(), argumentSpecs());
        Field f = out.getFields().get(0);
        if (f.getType() instanceof ArrowType.Null && f.getChildren().isEmpty()) {
            return BindResponse.forSchema(Schemas.singleResultAnyIpc());
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(out));
    }

    /**
     * Bind-time type-bound enforcement for {@code @Vector(any=true)} arguments.
     * Walks {@link #argumentSpecs()}, maps each non-const, non-table spec to its
     * input column, and rejects column types that fail the declared
     * {@link TypeBoundPredicate}. Skipped when {@code inputSchema} is absent
     * (catalog enumeration).
     */
    private void enforceTypeBounds(Schema inputSchema, List<ArgSpec> specs) {
        if (inputSchema == null || inputSchema.getFields().isEmpty()) return;
        int inputIdx = 0;
        for (ArgSpec spec : specs) {
            if (spec.isConst() || spec.tableInput()) continue;  // not in input batch
            if (inputIdx >= inputSchema.getFields().size()) break;
            List<TypeBoundPredicate> bounds = spec.typeBound();
            if (spec.varargs()) {
                // Apply bounds to every remaining input column.
                for (int j = inputIdx; j < inputSchema.getFields().size(); j++) {
                    if (!bounds.isEmpty()) {
                        checkBound(spec.name() + "[" + (j - inputIdx) + "]", bounds,
                                inputSchema.getFields().get(j).getType());
                    }
                }
                return;
            }
            if (!bounds.isEmpty()) {
                checkBound(spec.name(), bounds,
                        inputSchema.getFields().get(inputIdx).getType());
            }
            inputIdx++;
        }
    }

    private void checkBound(String argLabel, List<TypeBoundPredicate> bounds, ArrowType actual) {
        for (TypeBoundPredicate bound : bounds) {
            if (!matches(bound, actual)) {
                throw new IllegalArgumentException(
                        name() + ": " + argLabel + " must be " + bound.description()
                                + " (got " + TypeRules.sqlTypeName(actual) + ")");
            }
        }
    }

    private static boolean matches(TypeBoundPredicate bound, ArrowType t) {
        return switch (bound) {
            case IS_ADDABLE -> TypeRules.isAddable(t);
        };
    }

    /**
     * {@inheritDoc}
     *
     * <p>Binds the input columns, consts, and settings to {@code compute()}'s
     * parameters and invokes it via a cached {@link MethodHandle} into a
     * framework-allocated output vector.
     *
     * @param params the invocation arguments, output schema, and settings.
     * @param input the input batch (owned by the caller, do not close).
     * @param alloc allocator for the output root.
     * @return the produced output root.
     */
    @Override
    public final VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                            BufferAllocator alloc) {
        return plan.invoke(this, params, input, alloc);
    }

    // ---------------- reflection plan ----------------

    private static final class ComputePlan {
        final Method method;
        final List<ArgSpec> argSpecs;
        final ArrowType staticOutputType;
        final List<ParamBinder> binders;
        final int outputParamIndex;        // index in binders[] of the output binder
        final Class<?> outputVectorClass;  // concrete output vector class
        /**
         * Cached MethodHandle for the compute() body. Beats {@link Method#invoke}
         * substantially in tight per-batch loops: invocation cost drops from
         * full reflective dispatch (varargs unwrap + arg array copy + access
         * checks + boxing reflection) to a single indirect call the JIT can
         * inline through. Resolved once at plan creation; the underlying
         * {@code Method} is also kept (for diagnostics + name lookups).
         */
        final MethodHandle methodHandle;
        /**
         * Per-thread reusable argument array sized to {@code binders.size() + 1}
         * (slot 0 is the receiver for the instance method). Eliminates a per-batch
         * {@code new Object[]} allocation on the hot path. Cleared via
         * {@link Arrays#fill} after every invocation so we don't hold references
         * to vectors past the call (those vectors will be released by the IPC
         * writer; holding refs would defeat that release).
         */
        final ThreadLocal<Object[]> argsHolder;

        /**
         * Per-thread cached output {@link VectorSchemaRoot} reused across batches
         * on the same stream. Each call to {@code invoke()} returns a non-closing
         * wrapper around this root, so the framework's per-batch
         * {@code root.close()} in {@code RpcServer.flushCollector} drops back to
         * a no-op and the underlying ArrowBufs survive into the next batch.
         *
         * Cached by output {@link Schema}: the first batch on a thread allocates
         * the root; subsequent batches reuse it if the schema is unchanged. A
         * schema change (rare — only when {@code outputSchema()} is overridden
         * to depend on input or arguments) closes the old root and allocates a
         * fresh one.
         *
         * <p><strong>Lifetime caveat.</strong> The cache is never proactively
         * closed; on thread death the ThreadLocal entry becomes unreachable and
         * the underlying ArrowBufs go through Netty's pool's normal weak-ref
         * cleanup. For long-lived dispatcher threads serving many streams this
         * is the desired behaviour — one allocation amortized over the worker's
         * lifetime.
         */
        final ThreadLocal<CachedOutput> cachedOutput;

        private ComputePlan(Method method, List<ArgSpec> argSpecs, ArrowType staticOutputType,
                              List<ParamBinder> binders, int outputParamIndex,
                              Class<?> outputVectorClass) {
            this.method = method;
            this.argSpecs = argSpecs;
            this.staticOutputType = staticOutputType;
            this.binders = binders;
            this.outputParamIndex = outputParamIndex;
            this.outputVectorClass = outputVectorClass;
            try {
                this.methodHandle = MethodHandles.lookup().unreflect(method);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(
                        "ScalarFn: unable to unreflect compute() — make it public or open the module",
                        e);
            }
            int argsLen = binders.size() + 1;  // +1 for the instance receiver
            this.argsHolder = ThreadLocal.withInitial(() -> new Object[argsLen]);
            this.cachedOutput = new ThreadLocal<>();
        }

        static ComputePlan forClass(Class<?> cls) {
            Method m = findCompute(cls);
            if (m.getReturnType() != void.class) {
                throw new IllegalStateException(cls.getName()
                        + ".compute() must return void (framework allocates the output vector)");
            }
            Parameter[] params = m.getParameters();
            // Find the output parameter: the LAST parameter that is an Arrow
            // vector class AND is unannotated (no @Vector / @Const / @Setting / @OutputLength).
            int outputIdx = -1;
            for (int i = params.length - 1; i >= 0; i--) {
                Parameter p = params[i];
                if (isUnannotated(p) && FieldVector.class.isAssignableFrom(p.getType())) {
                    outputIdx = i;
                    break;
                }
            }
            if (outputIdx < 0) {
                throw new IllegalStateException(cls.getName()
                        + ".compute() must have an output vector parameter (last unannotated "
                        + "Arrow vector class, e.g. 'BigIntVector result')");
            }
            Class<?> outputClass = params[outputIdx].getType();
            ArrowType outputType = vectorClassToArrow(outputClass);

            List<ArgSpec> argSpecs = new ArrayList<>();
            List<ParamBinder> binders = new ArrayList<>(params.length);
            int position = 0;
            int constDeclIdx = 0;
            for (int i = 0; i < params.length; i++) {
                Parameter p = params[i];
                if (i == outputIdx) {
                    binders.add(new OutputBinder(outputClass));
                    continue;
                }
                Vector vec = p.getAnnotation(Vector.class);
                Const c = p.getAnnotation(Const.class);
                Setting s = p.getAnnotation(Setting.class);
                OutputLength ol = p.getAnnotation(OutputLength.class);
                if (vec != null) {
                    String wire = vec.value().isEmpty() ? snake(p.getName()) : vec.value();
                    if (vec.varargs() && vec.any()) {
                        argSpecs.add(new ArgSpec(wire, position++, new ArrowType.Null(), vec.doc(),
                                false, false, "", List.of(vec.typeBound()), true, true));
                        binders.add(new VarargsVectorBinder());
                    } else if (vec.varargs()) {
                        // Typed varargs: List<BigIntVector> → element type drives Arrow type
                        ArrowType eltType = varargsElementArrowType(p);
                        argSpecs.add(new ArgSpec(wire, position++, eltType, vec.doc(), true,
                                false, "", List.of(), true, false));
                        binders.add(new VarargsVectorBinder());
                    } else if (vec.any()) {
                        argSpecs.add(new ArgSpec(wire, position++, new ArrowType.Null(), vec.doc(),
                                false, false, "", List.of(vec.typeBound()), false, true));
                        binders.add(new VectorBinder(p.getType()));
                    } else {
                        ArrowType t = vectorClassToArrow(p.getType());
                        argSpecs.add(new ArgSpec(wire, position++, t, vec.doc(), false,
                                false, "", List.of(), false, false));
                        binders.add(new VectorBinder(p.getType()));
                    }
                } else if (c != null) {
                    String wire = c.value().isEmpty() ? snake(p.getName()) : c.value();
                    ArrowType t = javaTypeToArrow(p.getType());
                    argSpecs.add(new ArgSpec(wire, position++, t, c.doc(), true,
                                false, "", List.of(), false, false));
                    binders.add(new ConstBinder(p.getType(), constDeclIdx++));
                } else if (s != null) {
                    String key = s.value().isEmpty() ? snake(p.getName()) : s.value();
                    binders.add(new SettingBinder(p.getType(), key, s.default_()));
                } else if (ol != null) {
                    binders.add(new OutputLengthBinder());
                } else if (p.getType() == BufferAllocator.class) {
                    binders.add(new AllocatorBinder());
                } else {
                    throw new IllegalStateException(cls.getName() + ".compute(): parameter '"
                            + p.getName() + "' needs @Vector, @Const, @Setting, @OutputLength, "
                            + "or be the unannotated output vector / BufferAllocator");
                }
            }
            return new ComputePlan(m, List.copyOf(argSpecs), outputType, List.copyOf(binders),
                    outputIdx, outputClass);
        }

        VectorSchemaRoot invoke(ScalarFn fn, ScalarProcessParams params, VectorSchemaRoot input,
                                  BufferAllocator alloc) {
            int rows = input.getRowCount();
            Schema outSchema = fn.outputSchema(input.getSchema(), params.arguments());

            // Pull the cached output root for this thread, allocating only on
            // first use OR when the output schema differs from the previous
            // batch's. Reusing the root across batches cuts the per-batch
            // VectorSchemaRoot.allocateNew() + AllocationManager construction +
            // FieldVector allocFixedDataAndValidityBufs overhead the JFR
            // profile pinned at ~21% of worker RUNNABLE CPU on the scalar
            // multiply workload.
            CachedOutput cached = cachedOutput.get();
            if (cached == null || !cached.schema.equals(outSchema)) {
                if (cached != null) cached.root.close();
                VectorSchemaRoot fresh = VectorSchemaRoot.create(outSchema, Allocators.root());
                fresh.allocateNew();
                cached = new CachedOutput(outSchema, fresh);
                cachedOutput.set(cached);
            }
            VectorSchemaRoot outRoot = cached.root;
            FieldVector outputVec = outRoot.getVector("result");

            // Hot path: reuse the ThreadLocal args array; slot 0 is the receiver
            // for the unbound MethodHandle. Cleared in finally to release refs.
            Object[] args = argsHolder.get();
            args[0] = fn;
            int vectorIdx = 0;
            for (int i = 0; i < binders.size(); i++) {
                ParamBinder b = binders.get(i);
                if (b instanceof OutputBinder) {
                    args[i + 1] = outputVec;
                } else {
                    args[i + 1] = b.resolve(params, input, alloc, vectorIdx);
                    if (b instanceof VectorBinder) vectorIdx++;
                    if (b instanceof VarargsVectorBinder) vectorIdx = input.getFieldVectors().size();
                }
            }
            try {
                methodHandle.invokeWithArguments(args);
            } catch (RuntimeException re) {
                throw re;
            } catch (Error e) {
                throw e;
            } catch (Throwable t) {
                throw new RuntimeException(t);
            } finally {
                Arrays.fill(args, null);
            }
            outputVec.setValueCount(rows);
            outRoot.setRowCount(rows);
            // Hand the writer a non-closing view backed by the same FieldVector
            // instances. {@code RpcServer.flushCollector} calls {@code root.close()}
            // unconditionally after writing each batch; for the cached root we
            // want that close to be a no-op so the underlying ArrowBufs survive
            // to the next batch. The wrapper shares vectors with the cache so
            // the writer reads the freshly populated data.
            return new NonClosingVectorSchemaRoot(outSchema, outRoot.getFieldVectors(), rows);
        }

        /**
         * Per-thread cache entry. Holds the schema this root was allocated for
         * (used to invalidate when {@code outputSchema()} returns a different
         * shape) and the root itself.
         */
        private record CachedOutput(Schema schema, VectorSchemaRoot root) {}

        /**
         * Thin subclass that shares {@link FieldVector}s with a cached
         * {@link VectorSchemaRoot} but turns {@code close()} into a no-op so
         * the framework's per-batch close doesn't release the underlying
         * ArrowBufs. Lifecycle of the wrapped vectors belongs to the cache,
         * not to this wrapper. {@code writeBatch} only reads from the vectors,
         * so a non-closing view is wire-equivalent to the original root.
         */
        private static final class NonClosingVectorSchemaRoot extends VectorSchemaRoot {
            NonClosingVectorSchemaRoot(Schema schema, List<FieldVector> fieldVectors, int rowCount) {
                super(schema, fieldVectors, rowCount);
            }

            @Override
            public void close() {
                // No-op: the cache owns the vectors.
            }
        }

        private static Method findCompute(Class<?> cls) {
            for (Method m : cls.getDeclaredMethods()) {
                if (m.getName().equals("compute") && !m.isSynthetic() && !m.isBridge()) {
                    m.setAccessible(true);
                    return m;
                }
            }
            throw new IllegalStateException(cls.getName() + " must define a compute() method");
        }

        private static boolean isUnannotated(Parameter p) {
            return p.getAnnotation(Vector.class) == null
                    && p.getAnnotation(Const.class) == null
                    && p.getAnnotation(Setting.class) == null
                    && p.getAnnotation(OutputLength.class) == null;
        }
    }

    // ---------------- binders ----------------

    private sealed interface ParamBinder {
        Object resolve(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc,
                        int vectorIdx);
    }

    private record VectorBinder(Class<?> javaType) implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            return input.getFieldVectors().get(vectorIdx);
        }
    }

    private record VarargsVectorBinder() implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            // Collect all remaining columns from vectorIdx onward.
            List<FieldVector> vs = input.getFieldVectors();
            return new ArrayList<>(vs.subList(vectorIdx, vs.size()));
        }
    }

    private record ConstBinder(Class<?> javaType, int declIndex) implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            Arguments args = params.arguments();
            Object raw = declIndex < args.positional().size() ? args.positional().get(declIndex) : null;
            return coerce(raw, javaType);
        }
    }

    private record SettingBinder(Class<?> javaType, String key, String defaultStr) implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            Map<String, Object> settings = params.settings();
            Object raw = settings == null ? null : settings.get(key);
            if (raw == null && !defaultStr.isEmpty()) raw = parseDefault(defaultStr, javaType);
            return coerce(raw, javaType);
        }
    }

    private record OutputLengthBinder() implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            return input.getRowCount();
        }
    }

    private record AllocatorBinder() implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            return alloc;
        }
    }

    private record OutputBinder(Class<?> javaType) implements ParamBinder {
        public Object resolve(ScalarProcessParams params, VectorSchemaRoot input,
                                 BufferAllocator alloc, int vectorIdx) {
            throw new UnsupportedOperationException("OutputBinder resolved inline");
        }
    }

    // ---------------- type maps ----------------

    private static ArrowType varargsElementArrowType(Parameter p) {
        if (p.getType() != List.class) {
            throw new IllegalArgumentException("@Vector(varargs=true) parameter must be List<X>, got " + p.getType());
        }
        Type t = p.getParameterizedType();
        if (t instanceof ParameterizedType pt) {
            Type arg = pt.getActualTypeArguments()[0];
            if (arg instanceof Class<?> cls) return vectorClassToArrow(cls);
        }
        return new ArrowType.Null();
    }

    private static ArrowType vectorClassToArrow(Class<?> c) {
        if (c == BigIntVector.class) return Schemas.INT64;
        if (c == IntVector.class) return new ArrowType.Int(32, true);
        if (c == SmallIntVector.class) return new ArrowType.Int(16, true);
        if (c == TinyIntVector.class) return new ArrowType.Int(8, true);
        if (c == Float8Vector.class) return Schemas.FLOAT64;
        if (c == Float4Vector.class) return new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
        if (c == VarCharVector.class) return Schemas.UTF8;
        if (c == BitVector.class) return Schemas.BOOL;
        if (c == VarBinaryVector.class) return Schemas.BINARY;
        if (c == FieldVector.class) return new ArrowType.Null();  // generic, dynamic output expected
        // Nested types — placeholder; fixture must override argumentSpecs() to provide children.
        if (c == StructVector.class) return new ArrowType.Null();
        if (c == ListVector.class) return new ArrowType.Null();
        if (c == FixedSizeListVector.class) return new ArrowType.Null();
        throw new IllegalArgumentException("Unsupported Arrow vector class: " + c.getName());
    }

    private static ArrowType javaTypeToArrow(Class<?> c) {
        if (c == long.class || c == Long.class || c == int.class || c == Integer.class) return Schemas.INT64;
        if (c == double.class || c == Double.class) return Schemas.FLOAT64;
        if (c == String.class) return Schemas.UTF8;
        if (c == boolean.class || c == Boolean.class) return Schemas.BOOL;
        if (c == byte[].class) return Schemas.BINARY;
        throw new IllegalArgumentException("Cannot map Java type to ArrowType: " + c.getName());
    }

    private static Object coerce(Object raw, Class<?> target) {
        if (raw == null) return defaultForPrimitive(target);
        if (target == long.class || target == Long.class) {
            if (raw instanceof Number n) return n.longValue();
        }
        if (target == int.class || target == Integer.class) {
            if (raw instanceof Number n) return n.intValue();
        }
        if (target == double.class || target == Double.class) {
            if (raw instanceof Number n) return n.doubleValue();
        }
        if (target == boolean.class || target == Boolean.class) {
            if (raw instanceof Boolean b) return b;
        }
        if (target == String.class) {
            return raw.toString();
        }
        if (target == byte[].class && raw instanceof byte[] b) return b;
        return raw;
    }

    private static Object defaultForPrimitive(Class<?> c) {
        if (c == long.class) return 0L;
        if (c == int.class) return 0;
        if (c == double.class) return 0.0;
        if (c == boolean.class) return false;
        return null;
    }

    private static Object parseDefault(String s, Class<?> target) {
        if (target == long.class || target == Long.class) return Long.parseLong(s);
        if (target == int.class || target == Integer.class) return Integer.parseInt(s);
        if (target == double.class || target == Double.class) return Double.parseDouble(s);
        if (target == boolean.class || target == Boolean.class) return Boolean.parseBoolean(s);
        return s;
    }

    private static String snake(String camel) {
        StringBuilder sb = new StringBuilder(camel.length() + 4);
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c) && i > 0) sb.append('_');
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }
}
