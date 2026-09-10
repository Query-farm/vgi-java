// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Cross-language conformance for {@link PushdownFiltersEncoder}: bytes written
 * here are decoded by <em>vgi-python's</em> {@code deserialize_filters}, the
 * reference implementation of this wire format.
 *
 * <p>A Java-encoder/Java-decoder round trip (see
 * {@link PushdownFiltersEncoderTest}) proves the two halves agree with each
 * other, not that either agrees with the protocol — a shared misreading of the
 * spec round-trips perfectly. This test closes that gap by handing the bytes to
 * a decoder that was never written against this encoder.
 *
 * <p>It shells out to {@code uv run --project <vgi-python> python} with a small
 * script from the test resources, and asserts on the JSON that script prints.
 * The test <em>skips</em> when that toolchain isn't present (no {@code uv}, no
 * vgi-python checkout) so it never fails a CI run that has no Python side;
 * point {@code VGI_PYTHON_DIR} at the checkout to run it elsewhere.
 */
final class PushdownFiltersPythonConformanceTest {

    private static final ProjectedColumns COLUMNS =
            ProjectedColumns.of(List.of("n", "name", "score", "addr", "key"));

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @Timeout(180)
    void pythonDecodesEveryNodeShapeThisEncoderEmits(@TempDir Path tmp) throws Exception {
        Path project = pythonProject();
        assumeTrue(project != null, "vgi-python + uv not available; skipping conformance check");

        EncodedPushdownFilters encoded = PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.and(
                        FilterPredicate.ge(5L),
                        FilterPredicate.lt(100L)))
                .filter(COLUMNS.column("name"), FilterPredicate.or(
                        FilterPredicate.eq("berlin"),
                        FilterPredicate.isNull()))
                .filter(COLUMNS.column("score"), FilterPredicate.gt(2.5d))
                .filter(COLUMNS.column("addr"),
                        FilterPredicate.structField(1, "city", FilterPredicate.ne("paris")))
                .filter(COLUMNS.column("key"), FilterPredicate.isNotNull())
                .filter(COLUMNS.column("key"), FilterPredicate.joinKeys(List.of(10L, 20L, 30L)))
                .encode();

        JsonNode decoded = runPythonDecoder(project, tmp, encoded);

        assertEquals("vgi.duckdb.standard.v1", decoded.get("semantics").asText());
        JsonNode filters = decoded.get("predicates");
        assertEquals(6, filters.size());
        for (int i = 0; i < filters.size(); i++) {
            assertEquals("p" + i, filters.get(i).get("id").asText());
            assertEquals("required", filters.get(i).get("mode").asText());
            assertEquals("query", filters.get(i).get("source").asText());
        }

        JsonNode and = filters.get(0).get("expression");
        assertEquals("and", and.get("node").asText());
        assertEquals("ge", and.get("children").get(0).get("op").asText());
        assertEquals(5, and.get("children").get(0).get("right").get("value").asInt());
        assertEquals("n", and.get("children").get(1).get("left").get("column_name").asText());

        JsonNode or = filters.get(1).get("expression");
        assertEquals("or", or.get("node").asText());
        assertEquals("berlin", or.get("children").get(0).get("right").get("value").asText());
        assertEquals("is_null", or.get("children").get(1).get("node").asText());

        JsonNode score = filters.get(2).get("expression");
        assertEquals("gt", score.get("op").asText());
        assertEquals(2.5d, score.get("right").get("value").asDouble());

        JsonNode struct = filters.get(3).get("expression");
        assertEquals("field_ref", struct.get("left").get("node").asText());
        assertEquals("city", struct.get("left").get("field_name").asText());
        assertEquals("paris", struct.get("right").get("value").asText());

        JsonNode notNull = filters.get(4).get("expression");
        assertEquals("is_null", notNull.get("node").asText());
        assertEquals(true, notNull.get("negated").asBoolean());

        JsonNode join = filters.get(5).get("expression");
        assertEquals("in", join.get("node").asText());
        assertEquals(true, join.get("external").asBoolean());
        assertEquals(List.of(10, 20, 30), List.of(join.get("values").get(0).asInt(),
                join.get("values").get(1).asInt(), join.get("values").get(2).asInt()));
    }

    // ------------------------------------------------------------------
    // Harness
    // ------------------------------------------------------------------

    private JsonNode runPythonDecoder(Path project, Path tmp, EncodedPushdownFilters encoded)
            throws Exception {
        Path script = tmp.resolve("decode_filters.py");
        try (InputStream in = getClass().getResourceAsStream("decode_filters.py")) {
            Files.write(script, in.readAllBytes());
        }
        Path filters = tmp.resolve("filters.ipc");
        Files.write(filters, encoded.pushdownFilters());

        List<String> command = new ArrayList<>(List.of(
                "uv", "run", "--project", project.toString(), "python",
                script.toString(), filters.toString()));
        for (int i = 0; i < encoded.joinKeys().size(); i++) {
            Path keys = tmp.resolve("join_keys_" + i + ".ipc");
            Files.write(keys, encoded.joinKeys().get(i));
            command.add(keys.toString());
        }

        Process p = new ProcessBuilder(command)
                .redirectErrorStream(false)
                .redirectError(tmp.resolve("stderr.txt").toFile())
                .start();
        String stdout = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!p.waitFor(150, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("vgi-python decoder timed out");
        }
        if (p.exitValue() != 0) {
            throw new IllegalStateException("vgi-python decoder failed (exit " + p.exitValue()
                    + "):\n" + Files.readString(tmp.resolve("stderr.txt")));
        }
        return JSON.readTree(stdout);
    }

    /** The vgi-python checkout to decode with, or {@code null} when unavailable. */
    private static Path pythonProject() {
        String configured = System.getenv("VGI_PYTHON_DIR");
        Path project = configured != null && !configured.isEmpty()
                ? Path.of(configured)
                : Path.of(System.getProperty("user.home"), "Development", "vgi-python");
        if (!Files.isRegularFile(project.resolve("pyproject.toml"))) return null;
        return hasUv() ? project : null;
    }

    private static boolean hasUv() {
        try {
            Process p = new ProcessBuilder("uv", "--version")
                    .redirectErrorStream(true).start();
            return p.waitFor(30, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }
}
