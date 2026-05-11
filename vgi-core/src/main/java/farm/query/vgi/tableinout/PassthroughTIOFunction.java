// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.tableinout;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Base for table-in-out functions whose output schema equals their input
 * schema (echo, filter, repeat, exception-finalize, &c.). Mirrors the
 * vgi-python {@code TableInOutGenerator} default-bind shape.
 *
 * <p>Subclasses inherit a default {@link #onBind} that returns the input
 * schema (with an empty-schema fallback for catalog enumeration). They still
 * provide {@code name()}, {@code metadata()}, {@code argumentSpecs()}, and
 * {@code createExchange()} as usual.</p>
 */
public abstract class PassthroughTIOFunction implements TableInOutFunction {

    @Override
    public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(in));
    }
}
