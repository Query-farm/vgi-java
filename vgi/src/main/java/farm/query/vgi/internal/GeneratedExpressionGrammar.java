// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The deterministic scalar grammar the extension reads for a VIRTUAL generated column
 * ({@code vgi_column_expression.cpp} {@code Scalar(..., generated=true)}): unqualified
 * column references; NULL, Boolean, integer and string literals; {@code CAST}/{@code ::} to
 * BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT or VARCHAR; the six comparisons;
 * {@code AND}/{@code OR}/{@code NOT}; {@code IS [NOT] NULL}; {@code COALESCE}; and
 * {@code + - * % ||}, {@code abs}, {@code lower}, {@code upper}. Everything else (division,
 * decimal or float literals, {@code TRY_CAST}, {@code CASE}, {@code IN}, {@code LIKE},
 * subqueries, other functions) is refused, as the extension refuses it.
 */
final class GeneratedExpressionGrammar {

    private static final Set<String> FUNCTIONS = Set.of("abs", "lower", "upper", "coalesce");
    private static final Set<String> TYPES = Set.of("boolean", "bool", "logical", "tinyint", "int1",
            "smallint", "int2", "short", "integer", "int", "int4", "signed", "bigint", "int8", "long",
            "varchar", "text", "string", "char", "bpchar", "nvarchar");
    private static final Set<String> RESERVED = Set.of("select", "from", "where", "case", "when", "then",
            "else", "end", "in", "like", "ilike", "between", "exists", "try_cast", "and", "or", "not", "is",
            "as", "distinct", "filter", "over", "similar", "glob", "collate", "interval");

    private final String s;
    private int i;
    private int nodes;

    private GeneratedExpressionGrammar(String sql) {
        this.s = sql;
    }

    /**
     * Validates {@code sql} against the generated-column grammar.
     *
     * @param sql the expression
     * @throws IllegalArgumentException naming the construct DuckDB would refuse
     */
    static void validate(String sql) {
        if (sql == null || sql.isBlank() || sql.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("generated expression is empty or contains NUL");
        }
        GeneratedExpressionGrammar g = new GeneratedExpressionGrammar(sql);
        g.or(0);
        g.skip();
        if (g.i != sql.length()) {
            throw new IllegalArgumentException("unsupported generated expression syntax at '" + sql.substring(g.i) + "'");
        }
    }

    private void node(int depth) {
        if (depth > 64 || ++nodes > 4096) throw new IllegalArgumentException("generated expression is too complex");
    }

    private void or(int depth) {
        node(depth);
        and(depth + 1);
        while (keyword("or")) and(depth + 1);
    }

    private void and(int depth) {
        node(depth);
        not(depth + 1);
        while (keyword("and")) not(depth + 1);
    }

    private void not(int depth) {
        node(depth);
        if (keyword("not")) {
            not(depth + 1);
            return;
        }
        comparison(depth + 1);
        if (keyword("is")) {
            keyword("not");
            if (!keyword("null")) throw new IllegalArgumentException("only IS [NOT] NULL is supported");
        }
    }

    private void comparison(int depth) {
        node(depth);
        additive(depth + 1);
        skip();
        for (String op : List.of("<=", ">=", "<>", "!=", "==", "=", "<", ">")) {
            if (s.startsWith(op, i)) {
                i += op.length();
                additive(depth + 1);
                return;
            }
        }
    }

    private void additive(int depth) {
        node(depth);
        multiplicative(depth + 1);
        while (true) {
            skip();
            if (s.startsWith("||", i)) i += 2;
            else if (peek('+') || (peek('-') && !s.startsWith("->", i))) i++;
            else return;
            multiplicative(depth + 1);
        }
    }

    private void multiplicative(int depth) {
        node(depth);
        unary(depth + 1);
        while (true) {
            skip();
            if (peek('/')) throw new IllegalArgumentException("division is not supported in generated columns");
            if (s.startsWith("**", i) || peek('^')) throw new IllegalArgumentException("power is not supported");
            if (!peek('*') && !peek('%')) return;
            i++;
            unary(depth + 1);
        }
    }

    private void unary(int depth) {
        node(depth);
        skip();
        if (peek('-') || peek('+')) {
            i++;
            unary(depth + 1);
            return;
        }
        primary(depth + 1);
        while (true) {
            skip();
            if (!s.startsWith("::", i)) return;
            i += 2;
            type();
        }
    }

    private void primary(int depth) {
        node(depth);
        skip();
        if (i >= s.length()) throw new IllegalArgumentException("generated expression ends early");
        char c = s.charAt(i);
        if (c == '(') {
            i++;
            or(depth + 1);
            expect(')');
            return;
        }
        if (c == '\'') {
            i++;
            while (true) {
                if (i >= s.length()) throw new IllegalArgumentException("unterminated string literal");
                if (s.charAt(i++) == '\'') {
                    if (peek('\'')) i++;
                    else return;
                }
            }
        }
        if (Character.isDigit(c) || c == '.') {
            integer();
            return;
        }
        if (c == '"') {
            quoted();
            column();
            return;
        }
        if (!Character.isLetter(c) && c != '_') {
            throw new IllegalArgumentException("unsupported generated expression syntax at '" + s.substring(i) + "'");
        }
        String word = word();
        String lower = word.toLowerCase(Locale.ROOT);
        if (lower.equals("null") || lower.equals("true") || lower.equals("false")) return;
        if (lower.equals("cast")) {
            expect('(');
            or(depth + 1);
            if (!keyword("as")) throw new IllegalArgumentException("CAST requires AS <type>");
            type();
            expect(')');
            return;
        }
        if (RESERVED.contains(lower)) {
            throw new IllegalArgumentException(word.toUpperCase(Locale.ROOT)
                    + " is outside the generated-column grammar");
        }
        skip();
        if (peek('.')) throw new IllegalArgumentException("qualified names are not supported");
        if (peek('(')) {
            if (!FUNCTIONS.contains(lower)) {
                throw new IllegalArgumentException("function " + word + " is not supported in generated columns"
                        + " (only abs, lower, upper, coalesce and + - * % ||)");
            }
            i++;
            skip();
            if (keyword("distinct")) throw new IllegalArgumentException("DISTINCT is not supported");
            if (!peek(')')) {
                do {
                    skip();
                    int at = i;
                    if (Character.isLetter(peekChar()) || peekChar() == '_') {
                        word();
                        skip();
                        if (s.startsWith(":=", i) || s.startsWith("=>", i)) {
                            throw new IllegalArgumentException("named arguments are not supported");
                        }
                        i = at;
                    }
                    or(depth + 1);
                    skip();
                } while (consume(','));
            }
            expect(')');
            return;
        }
        column();
    }

    private void column() {
        skip();
        if (peek('.')) throw new IllegalArgumentException("qualified column references are not supported");
    }

    private void integer() {
        int start = i;
        while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        if (i < s.length() && (s.charAt(i) == '.' || s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
            throw new IllegalArgumentException("decimal and float literals are not supported in generated columns");
        }
        if (i == start) throw new IllegalArgumentException("malformed number");
        if (i < s.length() && (Character.isLetter(s.charAt(i)) || s.charAt(i) == '_')) {
            throw new IllegalArgumentException("malformed number");
        }
        try {
            Long.parseLong(s.substring(start, i));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("integer literal exceeds BIGINT");
        }
    }

    private void type() {
        skip();
        if (i < s.length() && s.charAt(i) == '"') {
            throw new IllegalArgumentException("quoted cast types are not supported");
        }
        String name = word().toLowerCase(Locale.ROOT);
        skip();
        if (peek('.')) throw new IllegalArgumentException("qualified cast types are not supported");
        if (!TYPES.contains(name)) {
            throw new IllegalArgumentException("cast to " + name.toUpperCase(Locale.ROOT)
                    + " is not supported (BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, VARCHAR)");
        }
        if (peek('[')) throw new IllegalArgumentException("list cast types are not supported");
    }

    private String word() {
        skip();
        int start = i;
        while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_' || s.charAt(i) == '$')) i++;
        if (i == start) throw new IllegalArgumentException("expected a name at '" + s.substring(i) + "'");
        return s.substring(start, i);
    }

    private void quoted() {
        i++;
        while (true) {
            if (i >= s.length()) throw new IllegalArgumentException("unterminated quoted identifier");
            if (s.charAt(i++) == '"') {
                if (peek('"')) i++;
                else return;
            }
        }
    }

    private boolean keyword(String k) {
        skip();
        int end = i + k.length();
        if (end > s.length() || !s.regionMatches(true, i, k, 0, k.length())) return false;
        if (end < s.length() && (Character.isLetterOrDigit(s.charAt(end)) || s.charAt(end) == '_')) return false;
        i = end;
        return true;
    }

    private boolean consume(char c) {
        skip();
        if (!peek(c)) return false;
        i++;
        return true;
    }

    private char peekChar() {
        return i < s.length() ? s.charAt(i) : '\0';
    }

    private boolean peek(char c) {
        return i < s.length() && s.charAt(i) == c;
    }

    private void expect(char c) {
        skip();
        if (!peek(c)) throw new IllegalArgumentException("expected '" + c + "' at '" + s.substring(i) + "'");
        i++;
    }

    private void skip() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }
}
