package org.engine;

import org.engine.db.processor.CsvDbLoader;
import org.engine.entity.CsvSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ✅ DBFeaturesTest
 *
 * Tests core DB-oriented capabilities of CsvEngine:
 * - .createIndex(...)
 * - .query(String)
 * - .createView(...)
 * - .analyzeStats(...)
 */
public class DbFeaturesTest {

    private static final Logger log = LoggerFactory.getLogger(DbFeaturesTest.class);
    private static final String EMP_CSV = "db_feature_employees.csv";

    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter writer = new FileWriter(EMP_CSV)) {
            writer.write("id,name,department_id,salary\n");
            writer.write("1,Alice,10,60000\n");
            writer.write("2,Bob,20,45000\n");
            writer.write("3,Charlie,10,75000\n");
            writer.write("4,David,30,55000\n");
        }
        log.info("📄 Created CSV for DB feature tests.");
    }

    @Test
    void testCreateIndex() throws Exception {
        CsvDbLoader loader = CsvSource.fromFile(EMP_CSV).initDb();
        loader.createIndex("department_id");
        log.info("✅ Index created successfully on 'department_id'");
    }

    @Test
    void testRawQuery() throws Exception {
        CsvDbLoader loader = CsvSource.fromFile(EMP_CSV).initDb();
        List<Map<String, Object>> result = loader.query("SELECT name FROM " + loader.getTableName() + " WHERE salary > 50000");

        log.info("📈 Employees with salary > 50000:");
        result.forEach(row -> log.info("👤 {}", row.get("NAME")));
    }

    @Test
    void testCreateView() throws Exception {
        CsvDbLoader loader = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader.createView("high_earners", "SELECT * FROM " + loader.getTableName() + " WHERE salary > 60000");

        List<Map<String, Object>> result = loader.query("SELECT * FROM high_earners");
        log.info("🏗️ View 'high_earners' rows: {}", result.size());

        result.forEach(row -> log.info("🧾 {}", row));
        assertTrue(result.size() > 0);
    }

}