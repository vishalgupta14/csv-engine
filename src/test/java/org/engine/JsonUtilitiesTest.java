package org.engine;

import com.fasterxml.jackson.databind.JsonNode;
import org.engine.entity.CsvSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ✅ JSON Utilities Test
 *
 * This test class validates:
 * - .asJsonList() -> List<JsonNode>
 * - .writeJsonToFile(path)
 * - .writeJsonLines(path)
 * - .parseJsonField("column")
 */
public class JsonUtilitiesTest {

    private static final Logger log = LoggerFactory.getLogger(JsonUtilitiesTest.class);
    private static final String BASIC_JSON_CSV = "test_employees.csv";
    private static final String EMBEDDED_JSON_CSV = "test_employees_with_metadata.csv";

    @BeforeAll
    static void setup() throws Exception {
        // Basic CSV
        try (FileWriter writer = new FileWriter(BASIC_JSON_CSV)) {
            writer.write("id,name,department_id,salary\n");
            writer.write("1,Alice,10,60000\n");
            writer.write("2,Bob,20,45000\n");
            writer.write("3,Charlie,10,75000\n");
        }

        // CSV with embedded JSON column
        try (FileWriter writer = new FileWriter(EMBEDDED_JSON_CSV)) {
            writer.write("id,name,metadata\n");
            writer.write("1,Alice,\"{\"\"role\"\":\"\"admin\"\",\"\"active\"\":true}\"\n");
            writer.write("2,Bob,\"{\"\"role\"\":\"\"user\"\",\"\"active\"\":false}\"\n");
        }

        log.info("📄 Sample CSVs created for JSON utility tests.");
    }

    @Test
    void testAsJsonList() {
        List<JsonNode> jsonList = CsvSource.fromFile(BASIC_JSON_CSV)
                .stream()
                .asJsonList();

        log.info("🧪 testAsJsonList() - JSON Rows:");
        jsonList.forEach(node -> log.info("📦 {}", node));

        assertEquals(3, jsonList.size());
        assertTrue(jsonList.get(0).has("name"));
        assertEquals("Alice", jsonList.get(0).get("name").asText());
    }

    @Test
    void testWriteJsonToFile() {
        String jsonFilePath = "employees.json";

        CsvSource.fromFile(BASIC_JSON_CSV)
                .stream()
                .writeJsonToFile(jsonFilePath);

        File file = new File(jsonFilePath);
        assertTrue(file.exists());
        assertTrue(file.length() > 0);

        log.info("💾 JSON array written to file: {}", file.getAbsolutePath());
    }

    @Test
    void testWriteJsonLines() throws Exception {
        String ndjsonPath = "employees.ndjson";

        CsvSource.fromFile(BASIC_JSON_CSV)
                .stream()
                .writeJsonLines(ndjsonPath);

        File file = new File(ndjsonPath);
        assertTrue(file.exists());

        List<String> lines = Files.readAllLines(file.toPath());
        log.info("📄 NDJSON lines written:");
        lines.forEach(line -> log.info("🧾 {}", line));

        assertEquals(3, lines.size());
        assertTrue(lines.get(0).contains("\"name\":\"Alice\""));
    }

    @Test
    void testParseJsonField() {
        String outputJsonFile = "parsed_metadata.json";

        CsvSource.fromFile(EMBEDDED_JSON_CSV)
                .stream()
                .parseJsonField("metadata")
                .writeJsonToFile(outputJsonFile);

        File file = new File(outputJsonFile);
        assertTrue(file.exists());

        log.info("🧬 Parsed embedded JSON field written to: {}", file.getAbsolutePath());
    }
}
