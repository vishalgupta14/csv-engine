package org.engine;

import org.engine.entity.CsvSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 📦 CsvSourceTest
 *
 * This test suite validates the core functionalities of CsvSource:
 * - Creating CsvSource from file (default/custom table name)
 * - Accessing file and table metadata
 * - In-memory CSV streaming and filtering
 * - Loading CSV into in-memory H2 database and running SQL queries
 * - Ensuring the test CSV file exists
 *
 * Uses emoji-annotated logs for better readability in terminal outputs.
 */
class BasicOperationForCsvSourceTest {

    private static final Logger log = LoggerFactory.getLogger(BasicOperationForCsvSourceTest.class);
    private static final String TEST_FILE = "test_employees.csv";

    /**
     * 📄 Creates a sample CSV file before all tests run.
     */
    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter writer = new FileWriter(TEST_FILE)) {
            writer.write("id,name,department_id,salary\n");
            writer.write("1,Alice,10,60000\n");
            writer.write("2,Bob,20,45000\n");
            writer.write("3,Charlie,10,75000\n");
            writer.write("4,David,30,55000\n");
        }
        log.info("📄 Test CSV file created successfully: {}", TEST_FILE);
    }

    /**
     * ✅ Validates:
     * - CsvSource loads correctly from file
     * - File and table name metadata are accessible
     */
    @Test
    void testFromFileAndGetters() {
        CsvSource source = CsvSource.fromFile(TEST_FILE);

        log.info("\n🧪 testFromFileAndGetters()");
        log.info("📂 File Path     : {}", source.getFile().getAbsolutePath());
        log.info("📋 Inferred Table: {}", source.getTableName());

        assertNotNull(source.getFile());
        assertEquals(TEST_FILE, source.getFile().getName());
        assertEquals("test_employees", source.getTableName());
    }

    /**
     * ✅ Validates:
     * - CsvSource accepts a custom table name
     */
    @Test
    void testCustomTableName() {
        CsvSource source = CsvSource.fromFile(TEST_FILE, "custom_table");

        log.info("\n🧪 testCustomTableName()");
        log.info("📝 Using custom table name: 📌 {}", source.getTableName());

        assertEquals("custom_table", source.getTableName());
    }

    /**
     * ✅ Validates:
     * - CsvSource stream() supports in-memory row streaming
     * - Filtering rows with salary > 50000 works as expected
     */
    @Test
    void testStreamFilter() {
        CsvSource source = CsvSource.fromFile(TEST_FILE);

        log.info("\n🧪 testStreamFilter()");
        long count = source.stream()
                .stream()
                .filter(row -> Integer.parseInt(row.get("salary")) > 50000)
                .count();

        log.info("🔍 Found {} 👤 employees with 💰 salary > 50000", count);
        assertEquals(3, count);
    }

    /**
     * ✅ Validates:
     * - CsvSource loads into in-memory H2 database using initDb()
     * - SQL queries work and return expected results
     */
    @Test
    void testInitDbAndQuery() throws Exception {
        CsvSource source = CsvSource.fromFile(TEST_FILE);

        log.info("\n🧪 testInitDbAndQuery()");
        List<Map<String, Object>> result = source.initDb()
                .query("SELECT name, salary FROM " + source.getTableName() + " WHERE salary > 50000");

        log.info("📊 Query Result:");
        result.forEach(row -> log.info("👤 {} | 💰 {}", row.get("NAME"), row.get("SALARY")));

        assertEquals(3, result.size());
        assertTrue(result.stream().anyMatch(row -> row.get("NAME").equals("Alice")));
        assertTrue(result.stream().anyMatch(row -> row.get("NAME").equals("Charlie")));
        assertTrue(result.stream().anyMatch(row -> row.get("NAME").equals("David")));
    }

    /**
     * ✅ Validates:
     * - The CSV file exists on disk and can be read by CsvSource
     */
    @Test
    void testFileExists() {
        CsvSource source = CsvSource.fromFile(TEST_FILE);
        File file = source.getFile();

        log.info("\n🧪 testFileExists()");
        log.info("📂 File exists check: {}", file.getAbsolutePath());

        assertTrue(file.exists());
    }
}
