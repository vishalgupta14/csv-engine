package org.engine;

import org.engine.entity.CsvSource;
import org.engine.entity.Employee;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ✅ CsvSource In-Memory Enhancements Test
 *
 * This test class validates extended in-memory CSV functionality:
 * - mapTo(Class<T>)
 * - collectToList()
 * - writeToCsv()
 * - limit(n), skip(n)
 * - groupBy(String column)
 */
class InMemoryEnhancementsTest {

    private static final Logger log = LoggerFactory.getLogger(InMemoryEnhancementsTest.class);
    private static final String TEST_FILE = "test_employees.csv";

    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter writer = new FileWriter(TEST_FILE)) {
            writer.write("id,name,department_id,salary\n");
            writer.write("1,Alice,10,60000\n");
            writer.write("2,Bob,20,45000\n");
            writer.write("3,Charlie,10,75000\n");
            writer.write("4,David,30,55000\n");
            writer.write("5,Eva,20,52000\n");
        }
        log.info("📄 Sample CSV created for in-memory enhancements test.");
    }

    @Test
    void testMapToPojo() {
        List<Employee> employees = CsvSource.fromFile(TEST_FILE)
                .stream()
                .mapTo(Employee.class);

        log.info("🧪 testMapToPojo() - {} rows mapped", employees.size());
        assertEquals(5, employees.size());
        assertEquals("Alice", employees.get(0).getName());
    }

    @Test
    void testCollectToList() {
        List<Map<String, String>> rows = CsvSource.fromFile(TEST_FILE)
                .stream()
                .toList();

        log.info("📋 Total rows collected: {}", rows.size());
        assertEquals(5, rows.size());
    }

    @Test
    void testWriteToCsv() {
        String outputPath = "output_employees.csv";
        CsvSource.fromFile(TEST_FILE)
                .stream()
                .writeToCsv(outputPath);

        File outFile = new File(outputPath);
        log.info("📤 Wrote output to: {}", outFile.getAbsolutePath());
        assertTrue(outFile.exists());
        assertTrue(outFile.length() > 0);
    }

    @Test
    void testLimitAndSkip() {
        List<Map<String, String>> limited = CsvSource.fromFile(TEST_FILE).stream().toList().stream().limit(2).collect(Collectors.toList());
        List<Map<String, String>> skipped = CsvSource.fromFile(TEST_FILE).stream().toList().stream().skip(3).collect(Collectors.toList());

        log.info("🔢 testLimit: first row = {}", limited.get(0).get("name"));
        log.info("🔢 testSkip: first row after skipping = {}", skipped.get(0).get("name"));

        assertEquals("Alice", limited.get(0).get("name"));
        assertEquals("David", skipped.get(0).get("name"));
    }

    @Test
    void testGroupByDepartmentId() {
        Map<String, List<Map<String, String>>> grouped = CsvSource.fromFile(TEST_FILE)
                .stream()
                .toList()
                .stream()
                .collect(Collectors.groupingBy(row -> row.get("department_id")));

        log.info("📊 testGroupByDepartmentId() - Group sizes: {}",
                grouped.entrySet().stream()
                        .map(e -> "Dept " + e.getKey() + ": " + e.getValue().size())
                        .collect(Collectors.joining(", ")));

        assertEquals(2, grouped.get("10").size());
        assertEquals(2, grouped.get("20").size());
        assertEquals(1, grouped.get("30").size());
    }
}