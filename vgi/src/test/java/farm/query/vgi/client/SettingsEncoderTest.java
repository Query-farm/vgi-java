// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.internal.SettingsParser;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link SettingsEncoder} against the {@code SettingsParser} it feeds. */
final class SettingsEncoderTest {

    @Test
    void roundTripsScalarSettingsOfEveryBasicType() {
        byte[] ipc = SettingsEncoder.builder()
                .setting("multiplier", 3L)
                .setting("scale_factor", 0.5d)
                .setting("greeting", "hi")
                .setting("verbose", true)
                .encode();

        Map<String, Object> parsed = SettingsParser.parse(ipc);
        assertEquals(3L, parsed.get("multiplier"));
        assertEquals(0.5d, parsed.get("scale_factor"));
        assertEquals("hi", parsed.get("greeting"));
        assertEquals(true, parsed.get("verbose"));
    }

    @Test
    void roundTripsAStructValuedSetting() {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        endpoint.put("host", "example.invalid");
        endpoint.put("port", 8080L);

        Map<String, Object> parsed =
                SettingsParser.parse(SettingsEncoder.builder().setting("endpoint", endpoint).encode());
        assertEquals(endpoint, parsed.get("endpoint"));
    }

    @Test
    void aNullSettingIsAbsentRatherThanNull() {
        // The parser drops null cells, so a client that means "unset" can send
        // either a typed null or nothing at all and the worker sees the same.
        Map<String, Object> parsed = SettingsParser.parse(SettingsEncoder.builder()
                .setting("present", 1L)
                .setting("absent", ScalarValue.ofNull(ScalarValue.UTF8))
                .encode());
        assertTrue(parsed.containsKey("present"));
        assertFalse(parsed.containsKey("absent"));
    }

    @Test
    void ofEncodesAWholeMap() {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("a", 1L);
        settings.put("b", "two");
        assertEquals(settings, SettingsParser.parse(SettingsEncoder.of(settings)));
    }
}
