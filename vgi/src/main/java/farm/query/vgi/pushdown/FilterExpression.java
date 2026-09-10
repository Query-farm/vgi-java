// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;

/** Typed expression tree for VGI Filter Encoding v2. */
public sealed interface FilterExpression {
    record ColumnRef(long columnIndex, String columnName, Field field) implements FilterExpression {}
    record FieldRef(FilterExpression expression, long fieldIndex, String fieldName, Field field)
            implements FilterExpression {}
    record Literal(long valueRef, Field field, Object value) implements FilterExpression {}
    record Comparison(ComparisonOperator op, FilterExpression left, FilterExpression right)
            implements FilterExpression {}
    record BooleanExpression(boolean conjunction, List<FilterExpression> children)
            implements FilterExpression {}
    record Not(FilterExpression expression) implements FilterExpression {}
    record IsNull(FilterExpression expression, boolean negated) implements FilterExpression {}
    sealed interface ValueSet permits LiteralSet, ExternalSet {}
    record LiteralSet(long valueRef, Field field, List<Object> values) implements ValueSet {}
    record ExternalSet(long batchIndex, long columnIndex, String columnName,
                       Field field, List<Object> values) implements ValueSet {}
    record In(FilterExpression expression, ValueSet set, boolean negated) implements FilterExpression {}
    record Cast(FilterExpression expression, long typeRef, Field field) implements FilterExpression {}
    enum ArithmeticOperator { ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO }
    record Arithmetic(ArithmeticOperator op, FilterExpression left, FilterExpression right)
            implements FilterExpression {}
    record Negate(FilterExpression expression) implements FilterExpression {}
    enum StandardFunction { STARTS_WITH, ENDS_WITH, CONTAINS, LIST_CONTAINS }
    record Call(Object function, List<FilterExpression> arguments, Map<String, Object> options)
            implements FilterExpression {}
    record RuntimeFilter(FilterIdentity algorithm, FilterExpression input, long artifactRef,
                         Field field, Object artifact, boolean passNulls, boolean supported)
            implements FilterExpression {}
}
