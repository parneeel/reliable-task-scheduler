package com.parneel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TaskScheduler {

    private final ExecutorService executor;
    private final PriorityBlockingQueue<Task> queue;
    private final TaskHandlerRegistry handlerRegistry;
    private final TaskRepository taskRepository;
    private final Semaphore semaphore;
    private final Metrics metrics;

    private final ScheduledExecutorService agingScheduler =
            Executors.newScheduledThreadPool(1);

    private final ScheduledExecutorService retryScheduler =
            Executors.newScheduledThreadPool(1);

    public TaskScheduler() {
        queue = new PriorityBlockingQueue<>(11, new TaskComparator());
        executor = Executors.newFixedThreadPool(4);

        handlerRegistry = new TaskHandlerRegistry();

        semaphore = new Semaphore(2);

        SendEmailHandler sendEmailHandler = new SendEmailHandler();
        handlerRegistry.register("SEND_EMAIL", sendEmailHandler);

        FailingTaskHandler failingTaskHandler = new FailingTaskHandler();
        handlerRegistry.register("FAIL_TASK", failingTaskHandler);

        metrics = new Metrics();

        DatabaseManager databaseManager = new DatabaseManager();

        try {
            databaseManager.createTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        taskRepository = new TaskRepository(databaseManager);

        // Recover unfinished tasks from the previous process.
        try {
            List<Task> recoveredTasks =
                    taskRepository.findUnfinishedTasks();

            for (Task task : recoveredTasks) {

                System.out.println(
                        "RECOVERED: " + task.getName()
                                + " (state=" + task.getState() + ")"
                );

                if (task.getState() == TaskState.RETRYING) {

                    long delay =
                            task.getNextRetryAt()
                                    - System.currentTimeMillis();

                    retryScheduler.schedule(
                            () -> {
                                try {
                                    task.setNextRetryAt(0);

                                    taskRepository.updateRetryInfo(
                                            task.getId(),
                                            "PENDING",
                                            task.getRetryCount(),
                                            0
                                    );

                                    queue.put(task);

                                    System.out.println(
                                            "RECOVERED RETRY READY: "
                                                    + task.getName()
                                    );

                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            },
                            Math.max(delay, 0),
                            TimeUnit.MILLISECONDS
                    );

                } else {
                    queue.put(task);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to recover tasks",
                    e
            );
        }

        // Start worker threads.
        for (int i = 0; i < 4; i++) {
            executor.submit(this::worker);
        }

        startAging();
    }

    public Future<String> submit(Task task) {

        task.setEnqueueTime();

        try {
            taskRepository.save(task);
        } catch (SQLException e) {
            task.getFuture().completeExceptionally(e);
            throw new RuntimeException(e);
        }

        queue.put(task);
        metrics.taskSubmitted();

        return task.getFuture();
    }

    private void worker() {

        while (true) {
            try {
                Task task = queue.take();
                executeTask(task);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void executeTask(Task task) throws InterruptedException {

        try {
            task.markRunning();

            taskRepository.updateState(
                    task.getId(),
                    "RUNNING"
            );

            TaskHandler handler =
                    handlerRegistry.getHandler(task.getTaskType());

            if (handler == null) {
                throw new IllegalStateException(
                        "No handler found for task type: "
                                + task.getTaskType()
                );
            }

            String result;

            semaphore.acquire();
            metrics.taskStarted();

            try {
                System.out.println(
                        "HANDLER STARTED: " + task.getName()
                );

                result = handler.execute(
                        task.getPayload()
                );

            } finally {
                semaphore.release();
                metrics.taskFinished();
            }

            task.markCompleted();

            taskRepository.updateState(
                    task.getId(),
                    "COMPLETED"
            );

            metrics.taskCompleted();

            task.getFuture().complete(result);

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            // Leave RUNNING state for crash recovery.
            throw e;

        } catch (Exception e) {

            int maxRetries = 3;

            int nextRetryCount =
                    task.getRetryCount() + 1;

            if (nextRetryCount <= maxRetries) {

                task.setRetryCount(nextRetryCount);

                long retryDelay =
                        1000L * (1L << (nextRetryCount - 1));

                task.setNextRetryAt(
                        System.currentTimeMillis()
                                + retryDelay
                );

                try {
                    taskRepository.updateRetryInfo(
                            task.getId(),
                            "RETRYING",
                            task.getRetryCount(),
                            task.getNextRetryAt()
                    );

                    metrics.taskRetried();

                } catch (SQLException dbException) {
                    dbException.printStackTrace();
                }

                scheduleRetry(task);

                System.out.println(
                        "TASK FAILED: "
                                + task.getName()
                                + " | retry "
                                + task.getRetryCount()
                );

            } else {

                task.markDead();

                try {
                    taskRepository.updateState(
                            task.getId(),
                            "DEAD"
                    );

                    metrics.taskDead();

                } catch (SQLException dbException) {
                    dbException.printStackTrace();
                }

                System.out.println(
                        "TASK DEAD: " + task.getName()
                );

                task.getFuture().completeExceptionally(e);
            }
        }
    }

    private void scheduleRetry(Task task) {

        long delay =
                task.getNextRetryAt()
                        - System.currentTimeMillis();

        retryScheduler.schedule(
                () -> {
                    try {
                        task.setNextRetryAt(0);

                        taskRepository.updateRetryInfo(
                                task.getId(),
                                "PENDING",
                                task.getRetryCount(),
                                0
                        );

                        queue.put(task);

                        System.out.println(
                                "RETRYING TASK: "
                                        + task.getName()
                                        + " | retry "
                                        + task.getRetryCount()
                        );

                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                },
                Math.max(delay, 0),
                TimeUnit.MILLISECONDS
        );
    }

    private void refreshAging() {

        List<Task> tasks = new ArrayList<>();

        queue.drainTo(tasks);

        for (Task task : tasks) {
            queue.put(task);
        }
    }

    private void startAging() {

        agingScheduler.scheduleAtFixedRate(
                this::refreshAging,
                0,
                1,
                TimeUnit.SECONDS
        );
    }

    public void shutdown() {

        executor.shutdownNow();
        agingScheduler.shutdown();
        retryScheduler.shutdown();

        metrics.printMetrics();
    }
}