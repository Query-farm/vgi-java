// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/**
 * Context for a {@code COPY ... TO} write, threaded onto {@link BindRequest}.
 *
 * <p>Present only when the bind/init opens a COPY-TO sink ({@code null}
 * otherwise — set by the VGI extension's {@code copy_to_bind}). The C++
 * extension serialises this as a nested, name-keyed Arrow {@code struct} field
 * on the bind request, so it is matched by component name and defaults to
 * {@code null} when the field is absent (ordinary scans/sinks). Mirrors
 * vgi-python's {@code vgi.protocol.CopyToContext}.
 *
 * <p>The handler's COPY options arrive through the normal
 * {@link BindRequest#arguments()} (built from the COPY options); the
 * <b>source</b> columns ride the existing {@link BindRequest#input_schema()}, so
 * neither is duplicated here.
 *
 * @param format    the {@code FORMAT} name resolved at COPY bind time
 * @param file_path the destination path from the {@code COPY ... TO 'path'} statement
 */
public record CopyToContext(
        String format,
        String file_path) implements ArrowSerializableRecord {}
