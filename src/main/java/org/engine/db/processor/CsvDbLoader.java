package org.engine.db.processor;

import java.io.File;
import java.sql.*;
import java.util.*;

import org.engine.db.connection.DatabaseBackend;
import org.engine.db.connection.H2Backend;
import org.engine.entity.JoinTarget;
import org.engine.enums.JoinType;
import org.engine.inmemory.processor.CsvInMemoryProcessor;
import org.engine.utils.DbUtil;
import org.engine.utils.CsvParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvDbLoader {

    private static final Logger log = LoggerFactory.getLogger(CsvInMemoryProcessor.class);
    private final File csvFile;
    private final String tableName;
    private final DatabaseBackend backend;

    public CsvDbLoader(File csvFile, String tableName) {
        this(csvFile, tableName, new H2Backend());
    }

    public CsvDbLoader(File csvFile, String tableName, DatabaseBackend backend) {
        this.csvFile = csvFile;
        this.tableName = tableName;
        this.backend = backend != null ? backend : new H2Backend();
    }

    public String getTableName() {
        return tableName;
    }

    public CsvDbLoader loadToDb() throws Exception {
        List<Map<String, String>> rows = CsvParserUtil.parseToMap(csvFile);
        DbUtil.loadToDb(rows, tableName, backend);
        return this;
    }

    public CsvDbLoader createIndex(String... columns) throws SQLException {
        try (Connection conn = backend.getConnection()) {
            String indexCols = String.join("_", columns);
            String colList = String.join(", ", columns);
            String sql = "CREATE INDEX IF NOT EXISTS idx_" + indexCols + " ON " + tableName + " (" + colList + ")";
            conn.createStatement().execute(sql);
        }
        return this;
    }

    public CsvDbLoader joinWith(CsvDbLoader other, String joinCondition, String resultViewName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL)) {
            // Get column names for both tables
            List<String> columnsA = DbUtil.getTableColumns(tableName,this.backend);
            List<String> columnsB = DbUtil.getTableColumns(other.tableName, other.backend);

            // Track duplicates and alias them
            Set<String> duplicates = new HashSet<>(columnsA);
            duplicates.retainAll(columnsB);

            List<String> selectList = new ArrayList<>();

            for (String col : columnsA) {
                String alias = duplicates.contains(col) ? "a_" + col : col;
                selectList.add("a." + col + " AS " + alias);
            }
            for (String col : columnsB) {
                String alias = duplicates.contains(col) ? "b_" + col : col;
                selectList.add("b." + col + " AS " + alias);
            }

            String sql = "CREATE VIEW " + resultViewName + " AS " +
                    "SELECT " + String.join(", ", selectList) +
                    " FROM " + tableName + " a JOIN " + other.tableName + " b ON " + joinCondition;

            conn.createStatement().execute(sql);
        }
        return this;
    }

    public CsvDbLoader joinWith(CsvDbLoader other, JoinType joinType, String joinCondition, String resultViewName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL)) {

            String leftAlias = "a";
            String rightAlias = "b";

            String leftTable = this.tableName;
            String rightTable = other.tableName;

            List<String> columnsA = DbUtil.getTableColumns(leftTable,this.backend);
            List<String> columnsB = DbUtil.getTableColumns(rightTable,other.backend);

            List<String> selectList = new ArrayList<>();

            for (String col : columnsA) {
                selectList.add(leftAlias + "." + col + " AS " + leftAlias + "_" + col);
            }

            for (String col : columnsB) {
                selectList.add(rightAlias + "." + col + " AS " + rightAlias + "_" + col);
            }

            String sql = "CREATE VIEW " + resultViewName + " AS " +
                    "SELECT " + String.join(", ", selectList) +
                    " FROM " + leftTable + " " + leftAlias +
                    " " + joinType.getSql() + " JOIN " + rightTable + " " + rightAlias +
                    (joinType.requiresOnCondition() ? " ON " + joinCondition : "");

            conn.createStatement().execute(sql);
        }
        return this;
    }

    public String joinWith(CsvDbLoader other, JoinType joinType, String joinCondition) throws SQLException {
        String randomViewName = "view_" + System.nanoTime();
        this.joinWith(other, joinType, joinCondition, randomViewName);
        return randomViewName;
    }

    public static CsvDbLoader joinMultiple(
            String viewName,
            CsvDbLoader base,
            List<JoinTarget> joins
    ) throws SQLException {

        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL)) {
            StringBuilder sql = new StringBuilder("CREATE VIEW " + viewName + " AS SELECT ");
            List<String> selectList = new ArrayList<>();

            String baseAlias = "a";
            List<String> baseColumns = DbUtil.getTableColumns(base.tableName, base.backend);
            for (String col : baseColumns) {
                selectList.add(baseAlias + "." + col + " AS " + baseAlias + "_" + col);
            }

            char alias = 'b';
            String fromClause = base.tableName + " " + baseAlias;

            for (JoinTarget jt : joins) {
                String currentAlias = String.valueOf(alias++);
                List<String> cols = DbUtil.getTableColumns(jt.table.tableName,base.backend);

                for (String col : cols) {
                    selectList.add(currentAlias + "." + col + " AS " + currentAlias + "_" + col);
                }

                fromClause += " " + jt.joinType.getSql() + " JOIN " +
                        jt.table.tableName + " " + currentAlias +
                        (jt.joinType.requiresOnCondition() ? " ON " + jt.joinCondition : "");
            }

            sql.append(String.join(", ", selectList)).append(" FROM ").append(fromClause);

            conn.createStatement().execute(sql.toString());
            return base;
        }
    }

    public CsvDbLoader unionWith(CsvDbLoader other, String resultViewName, boolean distinct) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL)) {

            // Get column names (must match for UNION to work)
            List<String> colsA = DbUtil.getTableColumns(this.tableName,this.backend);
            List<String> colsB = DbUtil.getTableColumns(other.tableName,other.backend);

            if (colsA.size() != colsB.size()) {
                throw new IllegalArgumentException("Both tables must have same number of columns for UNION.");
            }

            for (int i = 0; i < colsA.size(); i++) {
                if (!colsA.get(i).equalsIgnoreCase(colsB.get(i))) {
                    throw new IllegalArgumentException("Column mismatch: " + colsA.get(i) + " vs " + colsB.get(i));
                }
            }

            CsvDbLoader.dropViewIfExists(resultViewName);

            String columns = String.join(", ", colsA);
            String unionType = distinct ? "UNION" : "UNION ALL";

            String sql = "CREATE VIEW " + resultViewName + " AS " +
                    "SELECT " + columns + " FROM " + this.tableName + " " +
                    unionType + " " +
                    "SELECT " + columns + " FROM " + other.tableName;

            conn.createStatement().execute(sql);
            return this; // for chaining
        }
    }


    /**
     * 🔄 Drop a view if it exists.
     */
    public static void dropViewIfExists(String viewName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP VIEW IF EXISTS " + viewName);
            log.info("🗑️ Dropped view if existed: {}", viewName);
        }
    }

    /**
     * 👀 Preview first N rows from a view or table
     */
    public static void preview(String tableOrViewName, int limit) throws SQLException {
        String query = "SELECT * FROM " + tableOrViewName + " LIMIT " + limit;
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            log.info("\n👁️ Previewing first {} rows from: {}", limit, tableOrViewName);
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    row.append(meta.getColumnName(i)).append(": ").append(rs.getString(i)).append("\t");
                }
                log.info(row.toString());
            }
        }
    }

    /**
     * 📋 Print schema (column names & types) of a table or view
     */
    public static void printSchema(String tableOrViewName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + tableOrViewName + " LIMIT 1");
             ResultSet rs = stmt.executeQuery()) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            log.info("\n📋 Schema for: {}", tableOrViewName);
            for (int i = 1; i <= columnCount; i++) {
                log.info("🔸 {} : {}", meta.getColumnName(i), meta.getColumnTypeName(i));
            }
        }
    }


    public static void dropView(String viewName) {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP VIEW IF EXISTS " + viewName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop view: " + viewName, e);
        }
    }

    public CsvDbLoader unionWith(CsvDbLoader other, String resultViewName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL)) {
            String sql = "CREATE VIEW " + resultViewName + " AS " +
                         "SELECT * FROM " + tableName +
                         " UNION ALL " +
                         "SELECT * FROM " + other.tableName;
            conn.createStatement().execute(sql);
        }
        return this;
    }

    public List<Map<String, Object>> query(String sql) throws Exception {
        return DbUtil.queryFromDb(sql, backend);
    }

    public CsvDbLoader loadToH2() throws Exception {
        List<Map<String, String>> rows = CsvParserUtil.parseToMap(csvFile);
        DbUtil.loadToDb(rows, tableName,backend);
        return this;
    }

    /**
     * 🏗️ Create a custom SQL view manually
     */
    public static void createView(String viewName, String selectSql) throws SQLException {
        dropViewIfExists(viewName);
        String sql = "CREATE VIEW " + viewName + " AS " + selectSql;
        try (Connection conn = DriverManager.getConnection(DbUtil.JDBC_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("🏗️ Created view '{}': {}", viewName, selectSql);
        }
    }

}
