// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/**
 * {@code conditional_message(repeat_count BIGINT [const], message VARCHAR [const],
 * condition BOOLEAN) -> VARCHAR}: returns {@code message} repeated {@code repeat_count}
 * times when {@code condition} is true, else empty.
 */
public final class ConditionalMessageFunction extends ScalarFn {

    @Override public String name() { return "conditional_message"; }
    @Override public String description() { return "Returns repeated message when condition is true"; }

    public void compute(
            @Const(value = "repeat_count") long repeatCount,
            @Const String message,
            @Vector BitVector condition,
            VarCharVector result) {
        String repeated = (repeatCount <= 0 || message == null) ? "" : message.repeat((int) repeatCount);
        Text repeatedText = new Text(repeated);
        Text emptyText = new Text("");
        int rows = condition.getValueCount();
        for (int i = 0; i < rows; i++) {
            boolean cond = !condition.isNull(i) && condition.get(i) != 0;
            result.setSafe(i, cond ? repeatedText : emptyText);
        }
    }
}
