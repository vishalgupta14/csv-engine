package org.engine.utils;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import org.engine.db.connection.DatabaseBackend;

public class DbUtil {
    public static final String JDBC_URL = "jdbc:h2:mem:csvdb;DB_CLOSE_DELAY=-1";

    /**
     * Loads CSV rows into an in-memory database (e.g., H2 or user-provided).
     */
    public static void loadToDb(List<Map<String, String>> rows, String tableName, DatabaseBackend backend) throws Exception {
        if (rows.isEmpty()) return;

        try (Connection conn = backend.getConnection()) {
            Map<String, String> firstRow = rows.get(0);
            String columns = firstRow.keySet().stream()
                    .map(col -> sanitize(col) + " VARCHAR(255)")
                    .collect(Collectors.joining(", "));
            String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + columns + ")";
            conn.createStatement().execute(createTableSQL);

            String placeholders = firstRow.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));
            String insertSQL = "INSERT INTO " + tableName + " VALUES (" + placeholders + ")";
            try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                for (Map<String, String> row : rows) {
                    int i = 1;
                    for (String col : firstRow.keySet()) {
                        stmt.setString(i++, row.get(col));
                    }
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        }
    }

    /**
     * Executes a SQL query against a provided backend and returns the result.
     */
    public static List<Map<String, Object>> queryFromDb(String sql, DatabaseBackend backend) throws Exception {
        try (Connection conn = backend.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            List<Map<String, Object>> result = new ArrayList<>();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    row.put(meta.getColumnName(i), rs.getObject(i));
                }
                result.add(row);
            }
            return result;
        }
    }

    public static List<String> getTableColumns(String tableName, DatabaseBackend backend) throws SQLException {
        List<String> columns = new ArrayList<>();
        try (Connection conn = backend.getConnection();
             ResultSet rs = conn.getMetaData().getColumns(null, null, tableName.toUpperCase(), null)) {

            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    private static String sanitize(String col) {
        return col.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}

