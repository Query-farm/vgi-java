// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * Crash/hang fixtures exercising the C++ Sink+Source operator's failure paths.
 * Each fails in a specific phase. Mirrors the vgi-python {@code CrashOn*} /
 * {@code HangOnProcess} fixtures.
 */
public final class CrashFunctions {

    private CrashFunctions() {}

    /** SIGKILLs its own worker on the first {@code process()} call. */
    public static final class CrashOnProcess extends AbstractBufferAndDrain {
        private static final FunctionSpec SPEC = FunctionSpec.builder("crash_on_process")
                .metadata(FunctionMetadata.describe("Worker SIGKILLs itself during process (test)")
                        .withCategories("test", "crash"))
                .table("data")
                .build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            try {
                new ProcessBuilder("kill", "-9", String.valueOf(ProcessHandle.current().pid()))
                        .start().waitFor();
            } catch (Exception e) {
                Runtime.getRuntime().halt(137);  // fallback: abrupt exit
            }
            Runtime.getRuntime().halt(137);
            return params.executionId();  // unreachable
        }
    }

    /** Buffers normally; raises during {@code combine()}. */
    public static final class CrashOnCombine extends AbstractBufferAndDrain {
        private static final FunctionSpec SPEC = FunctionSpec.builder("crash_on_combine")
                .metadata(FunctionMetadata.describe("Worker raises during combine (test)")
                        .withCategories("test", "crash"))
                .table("data")
                .build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
            throw new RuntimeException("Intentional exception during combine()");
        }
    }

    /** Combine returns normally; finalize raises on the first tick. */
    public static final class CrashOnFinalize extends AbstractBufferAndDrain {
        private static final FunctionSpec SPEC = FunctionSpec.builder("crash_on_finalize")
                .metadata(FunctionMetadata.describe("Worker raises during finalize (test)")
                        .withCategories("test", "crash"))
                .table("data")
                .build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public farm.query.vgi.table.TableProducerState createFinalizeProducer(
                farm.query.vgi.buffering.TableBufferingFinalizeParams params) {
            return new farm.query.vgi.buffering.BufferingFinalizeProducer(params) {
                @Override public void produceTick(farm.query.vgirpc.OutputCollector out,
                                                      farm.query.vgirpc.CallContext ctx) {
                    throw new IllegalStateException("Intentional exception during finalize()");
                }
            };
        }
    }

    /** Sleeps an hour in {@code process()}; used by the manual cancellation smoke. */
    public static final class HangOnProcess extends AbstractBufferAndDrain {
        private static final FunctionSpec SPEC = FunctionSpec.builder("hang_on_process")
                .metadata(FunctionMetadata.describe("Worker sleeps in process (manual cancel test)")
                        .withCategories("test", "hang"))
                .table("data")
                .build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            try { Thread.sleep(3_600_000L); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return params.executionId();  // unreachable
        }
    }
}
