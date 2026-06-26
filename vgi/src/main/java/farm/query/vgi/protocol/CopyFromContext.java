// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Context for a {@code COPY ... FROM} read, threaded onto {@link BindRequest}.
 *
 * <p>Present only when the bind/init opens a COPY-FROM scan ({@code null}
 * otherwise — set by the VGI extension's {@code copy_from_bind}). The C++
 * extension serialises this as a nested, name-keyed Arrow {@code struct} field
 * on the bind request, so it is matched by component name and defaults to
 * {@code null} when the field is absent (ordinary scans). Mirrors vgi-python's
 * {@code vgi.protocol.CopyFromContext}.
 *
 * <p>The handler's COPY options arrive through the normal
 * {@link BindRequest#arguments()} (built from the COPY options), so they are not
 * duplicated here.
 *
 * @param format          the {@code FORMAT} name resolved at COPY bind time
 * @param file_path       the source path from the {@code COPY ... FROM 'path'} statement
 * @param expected_schema serialised Arrow {@code Schema} (IPC) of the COPY target's
 *     columns (name + type, in target order). The worker must bind its output to,
 *     and emit columns whose types match, this schema exactly — DuckDB inserts no
 *     cast between the scan and the INSERT
 */
public record CopyFromContext(
        String format,
        String file_path,
        byte[] expected_schema) implements ArrowSerializableRecord {}
