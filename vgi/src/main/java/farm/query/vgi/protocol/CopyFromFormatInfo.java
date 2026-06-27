// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.Map;

/**
 * A custom {@code COPY ... FROM} format advertised by a VGI catalog. Serialised
 * as an item in {@code catalog_copy_from_formats}; mirrors the C++
 * {@code CopyFromFormatInfoSchema} and vgi-python's {@code CopyFromFormatInfo}.
 *
 * <p>Field order, types and nullability are part of the wire contract (see
 * {@link farm.query.vgi.internal.CopyFromFormatInfoSerializer}).
 *
 * @param comment     optional free-text comment, or {@code null}
 * @param tags        arbitrary key/value metadata tags
 * @param format_name the {@code FORMAT} identifier users type (without the attach alias)
 * @param handler     registered name of the worker function that performs the read
 * @param options     IPC-encoded Arrow schema of the format's options, built from the
 *     handler's argument specs (same encoding as {@code FunctionInfo.arguments});
 *     each field's metadata carries the option type / default / {@code vgi_doc}
 * @param direction   {@code "from"} | {@code "to"} | {@code "both"} — which COPY direction(s)
 *     this format serves
 * @param description intrinsic documentation from the handler's metadata
 * @param ordered     COPY ... TO only — {@code true} when the writer requires rows in source
 *     order, so the extension installs a single-threaded sink ({@code REGULAR_COPY_TO_FILE});
 *     always {@code false} for readers. Set via {@code Meta.sink_order_dependent} on a
 *     {@link farm.query.vgi.table.CopyToFunction}
 */
public record CopyFromFormatInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String format_name,
        String handler,
        byte[] options,
        String direction,
        String description,
        boolean ordered) implements ArrowSerializableRecord {}
