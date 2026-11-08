// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ScanPlanningFiltersCompareTest {

    @Test
    void comparesNumbersByValueStringsByCodePointAndRefusesMixedTypes() {
        assertTrue(ScanPlanningFilters.compare("￿", "😀") < 0);
        assertTrue(ScanPlanningFilters.compare(1, 2.5) < 0);
        assertTrue(ScanPlanningFilters.compare(java.time.LocalDate.of(2026, 10, 3),
                java.time.LocalDate.of(2026, 10, 3).toEpochDay()) == 0);
        assertNull(ScanPlanningFilters.compare("a", 1L));
        assertNull(ScanPlanningFilters.compare(Double.NaN, 1L));
    }
}
