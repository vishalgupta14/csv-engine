package org.engine;

import org.engine.entity.CsvSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ✅ SchemaValidationTest
 *
 * This test class validates schema-related operations:
 * - getHeaders()
 * - inferSchema()
 * - checkRequiredColumns(...)
 * - validateAgainstSchema(...)
 */
public class SchemaValidationTest {

    private static final Logger log = LoggerFactory.getLogger(SchemaValidationTest.class);
    private static final String TEST_FILE = "schema_validation_employees.csv";

    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter writer = new FileWriter(TEST_FILE)) {
            writer.write("id,name,department_id,salary\n");
            writer.write("1,Alice,10,60000\n");
            writer.write("2,Bob,20,45000\n");
            writer.write("3,Charlie,10,75000\n");
        }
        log.info("📄 Sample CSV created for schema validation test.");
    }

    @Test
    void testGetHeaders() {
        List<String> headers = CsvSource.fromFile(TEST_FILE).stream().getHeaders();

        log.info("📋 Headers: {}", headers);
        assertEquals(List.of("id", "name", "department_id", "salary"), headers);
    }

    @Test
    void testInferSchema() {
        Map<String, String> schema = CsvSource.fromFile(TEST_FILE).stream().inferSchema(10);

        log.info("🧬 Inferred Schema:");
        schema.forEach((col, type) -> log.info("🔹 {} → {}", col, type));

        assertEquals("INTEGER", schema.get("id"));
        assertEquals("STRING", schema.get("name"));
        assertEquals("INTEGER", schema.get("department_id"));
        assertEquals("INTEGER", schema.get("salary"));
    }

    @Test
    void testCheckRequiredColumns() {
        assertDoesNotThrow(() -> CsvSource.fromFile(TEST_FILE).stream()
                .checkRequiredColumns("id", "name", "salary"));

        Exception ex = assertThrows(IllegalArgumentException.class, () ->
                CsvSource.fromFile(TEST_FILE).stream()
                        .checkRequiredColumns("nonexistent_column"));

        log.info("⚠️ Expected failure: {}", ex.getMessage());
    }

    @Test
    void testValidateAgainstExpectedSchema() {
        Map<String, String> expected = Map.of(
                "id", "INTEGER",
                "name", "STRING",
                "department_id", "INTEGER",
                "salary", "INTEGER"
        );

        boolean valid = CsvSource.fromFile(TEST_FILE)
                .stream()
                .validateAgainstSchema(expected, 5);

        assertTrue(valid);
    }

    @Test
    void testSchemaValidationFails() {
        Map<String, String> wrongSchema = Map.of(
                "id", "STRING",   // Expected mismatch
                "name", "STRING",
                "salary", "DOUBLE"
        );

        boolean result = CsvSource.fromFile(TEST_FILE)
                .stream()
                .validateAgainstSchema(wrongSchema, 5);

        assertFalse(result);
    }
}
