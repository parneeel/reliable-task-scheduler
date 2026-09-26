package com.parneel;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TaskRepository {

    private final DatabaseManager databaseManager;

    public TaskRepository(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    public void findAll() throws SQLException {

        String sql = "SELECT * FROM tasks";

        try (Connection connection = databaseManager.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {

                System.out.println(
                        resultSet.getLong("id") + " | " +
                                resultSet.getString("name") + " | " +
                                resultSet.getString("state")
                );
            }
        }
    }

    public void save(Task task) throws SQLException {

        String sql = """
                INSERT INTO tasks
                (name, task_type, payload, weight, state, enqueue_time,
                 retry_count, next_retry_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (Connection connection = databaseManager.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(
                             sql,
                             Statement.RETURN_GENERATED_KEYS)) {

            statement.setString(1, task.getName());
            statement.setString(2, task.getTaskType());
            statement.setString(3, task.getPayload());
            statement.setInt(4, task.getWeight());
            statement.setString(5, task.getState().name());
            statement.setLong(6, task.getEnqueueTime());
            statement.setInt(7, task.getRetryCount());
            statement.setLong(8, task.getNextRetryAt());

            statement.executeUpdate();

            try (ResultSet resultSet = statement.getGeneratedKeys()) {
                if (resultSet.next()) {
                    task.setId(resultSet.getLong(1));
                }
            }
        }
    }

    public void updateState(
            long taskId,
            String state
    ) throws SQLException {

        String sql = """
                UPDATE tasks
                SET state = ?
                WHERE id = ?
                """;

        try (Connection connection = databaseManager.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, state);
            statement.setLong(2, taskId);

            statement.executeUpdate();
        }
    }

    public void updateRetryInfo(
            long taskId,
            String state,
            int retryCount,
            long nextRetryAt
    ) throws SQLException {

        String sql = """
                UPDATE tasks
                SET state = ?,
                    retry_count = ?,
                    next_retry_at = ?
                WHERE id = ?
                """;

        try (Connection connection = databaseManager.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, state);
            statement.setInt(2, retryCount);
            statement.setLong(3, nextRetryAt);
            statement.setLong(4, taskId);

            statement.executeUpdate();
        }
    }

    public List<Task> findUnfinishedTasks()
            throws SQLException {

        String sql = """
                SELECT *
                FROM tasks
                WHERE state IN ('PENDING', 'RUNNING', 'RETRYING')
                """;

        List<Task> tasks = new ArrayList<>();

        try (Connection connection = databaseManager.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {

                Task task = new Task(
                        resultSet.getString("name"),
                        resultSet.getInt("weight"),
                        resultSet.getString("task_type"),
                        resultSet.getString("payload")
                );

                task.setId(
                        resultSet.getLong("id")
                );

                task.setEnqueueTime(
                        resultSet.getLong("enqueue_time")
                );

                task.setRetryCount(
                        resultSet.getInt("retry_count")
                );

                task.setNextRetryAt(
                        resultSet.getLong("next_retry_at")
                );

                task.setState(
                        TaskState.valueOf(
                                resultSet.getString("state")
                        )
                );

                tasks.add(task);
            }
        }

        return tasks;
    }
}