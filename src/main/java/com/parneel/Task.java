package com.parneel;

import java.util.concurrent.CompletableFuture;

public class Task {

    private long id;
    private final String name;
    private final int weight;
    private final CompletableFuture<String> future;

    private long enqueueTime;
    private TaskState state;

    private final String taskType;
    private final String payload;

    private int retryCount;
    private long nextRetryAt;

    public Task(
            String name,
            int weight,
            String taskType,
            String payload
    ) {
        this.name = name;
        this.weight = weight;
        this.future = new CompletableFuture<>();
        this.state = TaskState.PENDING;
        this.taskType = taskType;
        this.payload = payload;
        this.retryCount = 0;
        this.nextRetryAt = 0;
    }

    public void setEnqueueTime() {
        this.enqueueTime = System.currentTimeMillis();
    }

    public void setEnqueueTime(long enqueueTime) {
        this.enqueueTime = enqueueTime;
    }

    public long getEffectivePriority() {
        long waitingTime =
                System.currentTimeMillis() - enqueueTime;

        long agingBonus = waitingTime / 1000;

        return weight + agingBonus;
    }

    public void markRunning() {
        if (state == TaskState.PENDING) {
            state = TaskState.RUNNING;
        }
    }

    public void markCompleted() {
        if (state == TaskState.RUNNING) {
            state = TaskState.COMPLETED;
        }
    }

    public void markDead() {
        if (state == TaskState.RUNNING) {
            state = TaskState.DEAD;
        }
    }

    public String getName() {
        return name;
    }

    public int getWeight() {
        return weight;
    }

    public TaskState getState() {
        return state;
    }

    public CompletableFuture<String> getFuture() {
        return future;
    }

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getPayload() {
        return payload;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getNextRetryAt() {
        return nextRetryAt;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void setNextRetryAt(long nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public void setState(TaskState state) {
        this.state = state;
    }
}