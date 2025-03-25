package org.engine;

import org.engine.db.processor.CsvDbLoader;
import org.engine.entity.CsvSource;
import org.engine.entity.JoinTarget;
import org.engine.enums.JoinType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JoinMultipleTablesTest {

    private static final Logger log = LoggerFactory.getLogger(JoinMultipleTablesTest.class);
    private static final String EMP_CSV = "join3_employees.csv";
    private static final String DEPT_CSV = "join3_departments.csv";
    private static final String LOC_CSV = "join3_locations.csv";

    @BeforeAll
    static void setup() throws Exception {
        try (FileWriter emp = new FileWriter(EMP_CSV)) {
            emp.write("id,name,department_id,salary\n");
            emp.write("1,Alice,10,60000\n");
            emp.write("2,Bob,20,45000\n");
            emp.write("3,Charlie,10,75000\n");
        }

        try (FileWriter dept = new FileWriter(DEPT_CSV)) {
            dept.write("id,department_name,location_id\n");
            dept.write("10,Engineering,100\n");
            dept.write("20,HR,200\n");
        }

        try (FileWriter loc = new FileWriter(LOC_CSV)) {
            loc.write("id,city\n");
            loc.write("100,New York\n");
            loc.write("200,London\n");
        }

        log.info("📄 Sample CSVs created for 3-way join test.");
    }

    @Test
    void testJoinMultipleTables() throws Exception {
        // Load tables
        CsvDbLoader emp = CsvSource.fromFile(EMP_CSV).initDb();
        CsvDbLoader dept = CsvSource.fromFile(DEPT_CSV).initDb();
        CsvDbLoader loc = CsvSource.fromFile(LOC_CSV).initDb();

        // Perform 3-way join: emp → dept → loc
        String viewName = "emp_dept_loc_" + System.nanoTime();

        CsvDbLoader.joinMultiple(viewName, emp, List.of(
            new JoinTarget(dept, JoinType.INNER, "a.department_id = b.id"),
            new JoinTarget(loc, JoinType.LEFT, "b.location_id = c.id")
        ));

        List<Map<String, Object>> result = emp.query("SELECT * FROM " + viewName);

        log.info("🔗 Multi-join [{}] result rows: {}", viewName, result.size());
        result.forEach(row -> log.info("🧾 {}", row));

    }
}
