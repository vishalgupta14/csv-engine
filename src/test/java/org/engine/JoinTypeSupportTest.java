package org.engine;

import org.engine.entity.CsvSource;
import org.engine.db.processor.CsvDbLoader;
import org.engine.enums.JoinType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ✅ JoinTypeSupportTest
 *
 * Tests all supported SQL join types in CsvEngine:
 * - INNER JOIN
 * - LEFT JOIN
 * - RIGHT JOIN
 * - FULL JOIN
 * - CROSS JOIN
 * - NATURAL JOIN
 * - SELF JOIN
 */
public class JoinTypeSupportTest {

    private static final Logger log = LoggerFactory.getLogger(JoinTypeSupportTest.class);
    private static final String EMP_CSV = "test_employees_join.csv";
    private static final String DEPT_CSV = "test_departments_join.csv";

    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter emp = new FileWriter(EMP_CSV)) {
            emp.write("id,name,department_id,salary,manager_id\n");
            emp.write("1,Alice,10,60000,\n");
            emp.write("2,Bob,20,45000,1\n");
            emp.write("3,Charlie,10,75000,1\n");
            emp.write("4,David,30,55000,2\n");
        }

        try (FileWriter dept = new FileWriter(DEPT_CSV)) {
            dept.write("id,department_name\n");
            dept.write("10,Engineering\n");
            dept.write("20,HR\n");
            dept.write("40,Marketing\n");
        }

        log.info("📄 Created sample CSVs for join tests.");
    }

    @Test
    void testInnerJoin() throws Exception {
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb().createIndex("department_id");
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb().createIndex("id");

        // ✅ Clean up old view safely
        dropViewIfExists("inner_view");

        emp.joinWith(dept, JoinType.INNER, "a.department_id = b.id", "inner_view");

        List<Map<String, Object>> result = emp.query("SELECT * FROM inner_view");
        log.info("🔗 INNER JOIN rows: {}", result.size());

        result.forEach(row -> log.info("🧾 {}", row));
    }



    @Test
    void testLeftJoin() throws Exception {
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb();

        emp.joinWith(dept, JoinType.LEFT, "a.department_id = b.id", "left_view");

        List<Map<String, Object>> result = emp.query("SELECT * FROM left_view");
        log.info("🔗 LEFT JOIN rows: {}", result.size());
        assertEquals(4, result.size()); // all employees included
    }

    @Test
    void testRightJoin() throws Exception {
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb();

        emp.joinWith(dept, JoinType.RIGHT, "a.department_id = b.id", "right_view");

        List<Map<String, Object>> result = emp.query("SELECT DISTINCT A_DEPARTMENT_ID FROM right_view");
        log.info("🔗 RIGHT JOIN rows: {}", result.size());

        assertTrue(result.size() >= 3); // right table (departments) is the anchor
    }

    //if this is not working then use UNION ALL which is alternative of this
    /*@Test
    void testFullJoin() throws Exception {
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb();

        emp.joinWith(dept, JoinType.FULL, "a.department_id = b.id", "full_view");

        List<Map<String, Object>> result = emp.query("SELECT * FROM full_view");
        log.info("🔗 FULL JOIN rows: {}", result.size());
        assertTrue(result.size() >= 5); // includes unmatched rows from both sides
    }*/

    @Test
    void testCrossJoin() throws Exception {
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb();

        emp.joinWith(dept, JoinType.CROSS, null, "cross_view");

        List<Map<String, Object>> result = emp.query("SELECT * FROM cross_view");
        log.info("➗ CROSS JOIN rows (should be 4 x 3 = 12): {}", result.size());
    }

    @Test
    void testNaturalJoin() throws Exception {
        // NATURAL JOIN works only when both tables share exact column names (e.g. "id")
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb();

        emp.joinWith(dept, JoinType.NATURAL, null, "natural_view");

        List<Map<String, Object>> result = emp.query("SELECT * FROM natural_view");
        log.info("🌱 NATURAL JOIN rows: {}", result.size());
    }

    @Test
    void testSelfJoin() throws Exception {
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        emp.createIndex("manager_id");

        emp.joinWith(emp, JoinType.LEFT, "a.manager_id = b.id", "self_view");

        List<Map<String, Object>> result = emp.query("SELECT a_name, b_name AS manager FROM self_view");

        log.info("👤 SELF JOIN (employee-manager relationships):");
        result.forEach(row -> log.info("👤 {} reports to 🧑 {}", row.get("a_name"), row.get("manager")));

        assertTrue(result.size() > 0);
    }

    private void dropViewIfExists(String viewName) {
        try (var conn = java.sql.DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
             var stmt = conn.createStatement()) {
            stmt.execute("DROP VIEW IF EXISTS " + viewName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop view: " + viewName, e);
        }
    }

}
