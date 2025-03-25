package org.engine.db.connection;

import org.engine.utils.DbUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class H2Backend implements DatabaseBackend {
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DbUtil.JDBC_URL); // <<< Fix here
    }

    public String getType() {
        return "H2";
    }
}

