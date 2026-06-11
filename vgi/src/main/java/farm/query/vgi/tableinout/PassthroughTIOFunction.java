// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Base for table-in-out functions whose output schema equals their input
 * schema (echo, filter, repeat, exception-finalize, etc.). Mirrors the
 * vgi-python {@code TableInOutGenerator} default-bind shape.
 *
 * <p>Subclasses inherit a default {@link #onBind} that returns the input
 * schema (with an empty-schema fallback for catalog enumeration). They still
 * provide {@code name()}, {@code metadata()}, {@code argumentSpecs()}, and
 * {@code createExchange()} as usual.</p>
 */
public abstract class PassthroughTIOFunction implements TableInOutFunction {

    /** Sole constructor; the passthrough bind needs no per-instance state. */
    protected PassthroughTIOFunction() {}

    /**
     * Returns the input schema as the output schema, falling back to an empty
     * schema when no input is present (catalog enumeration).
     *
     * @param params the bind-time arguments and input schema.
     * @return a bind response echoing the input schema.
     */
    @Override
    public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(in));
    }
}
