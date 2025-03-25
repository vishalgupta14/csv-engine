package org.engine.db.connection;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseBackend {
    Connection getConnection() throws SQLException;
    String getType(); // e.g., "H2", "Postgres"
}
