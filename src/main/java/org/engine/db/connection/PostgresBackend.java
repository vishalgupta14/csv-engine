package org.engine.db.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresBackend implements DatabaseBackend {
    private final String url, user, pass;

    public PostgresBackend(String url, String user, String pass) {
        this.url = url; this.user = user; this.pass = pass;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, pass);
    }

    public String getType() {
        return "Postgres";
    }
}
