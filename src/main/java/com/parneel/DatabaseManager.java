package com.parneel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {

    public Connection getConnection() throws SQLException {

        Connection connection =
                DriverManager.getConnection(
                        "jdbc:sqlite:scheduler.db"
                );

        try (Statement statement = connection.createStatement()) {

            statement.execute(
                    "PRAGMA journal_mode=WAL"
            );

            statement.execute(
                    "PRAGMA busy_timeout=5000"
            );
        }

        return connection;
    }

    public void createTable() throws SQLException {

        String sql = """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    payload TEXT,
                    weight INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    enqueue_time INTEGER NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    next_retry_at INTEGER NOT NULL DEFAULT 0
                )
                """;

        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(sql);
        }
    }
}