package com.parneel;

public class Main {

    public static void main(String[] args) {

        TaskScheduler scheduler = new TaskScheduler();

        for (int i = 1; i <= 10; i++) {

            Task task = new Task(
                    "Task-" + i,
                    5,
                    "SEND_EMAIL",
                    "user" + i + "@gmail.com"
            );

            scheduler.submit(task);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        scheduler.shutdown();
    }
}