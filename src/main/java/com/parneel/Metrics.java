package com.parneel;

import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {
    private final AtomicInteger submitted;
    private final AtomicInteger completed;
    private final AtomicInteger retried;
    private final AtomicInteger dead;
    private final AtomicInteger currentlyRunning;

    public Metrics(){
        submitted = new AtomicInteger(0);
        completed = new AtomicInteger(0);
        retried = new AtomicInteger(0);
        dead = new AtomicInteger(0);
        currentlyRunning = new AtomicInteger(0);
    }
    public void printMetrics() {
        System.out.println("===== METRICS =====");
        System.out.println("Submitted: " + getSubmitted());
        System.out.println("Completed: " + getCompleted());
        System.out.println("Retried: " + getRetried());
        System.out.println("Dead: " + getDead());
        System.out.println("Currently Running: " + getCurrentlyRunning());
        System.out.println("===================");
    }

    public void taskSubmitted() {
        submitted.incrementAndGet();
    }

    public void taskCompleted() {
        completed.incrementAndGet();
    }

    public void taskRetried() {
        retried.incrementAndGet();
    }

    public void taskDead() {
        dead.incrementAndGet();
    }

    public void taskStarted() {
        currentlyRunning.incrementAndGet();
    }

    public void taskFinished() {
        currentlyRunning.decrementAndGet();
    }

    public int getSubmitted() {
        return submitted.get();
    }

    public int getCompleted() {
        return completed.get();
    }

    public int getRetried() {
        return retried.get();
    }

    public int getDead() {
        return dead.get();
    }

    public int getCurrentlyRunning() {
        return currentlyRunning.get();
    }
}
