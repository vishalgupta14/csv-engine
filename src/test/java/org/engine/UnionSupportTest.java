package org.engine;

import org.engine.db.processor.CsvDbLoader;
import org.engine.entity.CsvSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ✅ UnionSupportTest
 *
 * Tests UNION and UNION ALL functionality in CsvEngine:
 * - Validate unionWith with distinct rows (UNION)
 * - Validate unionWith with duplicates retained (UNION ALL)
 */
public class UnionSupportTest {

    private static final Logger log = LoggerFactory.getLogger(UnionSupportTest.class);
    private static final String EMP_CSV_2022 = "union_employees_2022.csv";
    private static final String EMP_CSV_2023 = "union_employees_2023.csv";

    /**
     * 📄 Setup two employee CSV files with overlapping structure and values
     */
    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter emp2022 = new FileWriter(EMP_CSV_2022)) {
            emp2022.write("id,name,department_id,salary\n");
            emp2022.write("1,Alice,10,60000\n");
            emp2022.write("2,Bob,20,45000\n");
        }

        try (FileWriter emp2023 = new FileWriter(EMP_CSV_2023)) {
            emp2023.write("id,name,department_id,salary\n");
            emp2023.write("2,Bob,20,45000\n"); // duplicate
            emp2023.write("3,Charlie,10,75000\n");
        }

        log.info("📄 Sample CSVs created for union tests.");
    }

    /**
     * 🔄 Test UNION (distinct only)
     * Should remove duplicates across both tables
     */
    @Test
    void testUnionDistinct() throws Exception {
        CsvDbLoader emp2022 = CsvSource.fromFile(EMP_CSV_2022).initDb();
        CsvDbLoader emp2023 = CsvSource.fromFile(EMP_CSV_2023).initDb();

        emp2022.unionWith(emp2023, "union_distinct", true);

        List<Map<String, Object>> result = emp2022.query("SELECT * FROM union_distinct ORDER BY id");

        log.info("🔁 UNION result (distinct): {} rows", result.size());
        result.forEach(row -> log.info("🧾 {}", row));

        assertEquals(3, result.size());
        assertTrue(result.stream().anyMatch(row -> row.get("NAME").equals("Charlie")));
    }

    /**
     * 🔁 Test UNION ALL (include duplicates)
     * Should include all rows from both tables, including duplicates
     */
    @Test
    void testUnionAll() throws Exception {
        CsvDbLoader emp2022 = CsvSource.fromFile(EMP_CSV_2022).initDb();
        CsvDbLoader emp2023 = CsvSource.fromFile(EMP_CSV_2023).initDb();

        emp2022.unionWith(emp2023, "union_all", false);

        List<Map<String, Object>> result = emp2022.query("SELECT * FROM union_all ORDER BY id");

        log.info("🔁 UNION ALL result (with duplicates): {} rows", result.size());
        result.forEach(row -> log.info("🧾 {}", row));

    }
}
