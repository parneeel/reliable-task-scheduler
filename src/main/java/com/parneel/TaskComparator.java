package com.parneel;

import java.util.Comparator;

public class TaskComparator implements Comparator<Task> {

    @Override
    public int compare(Task task1, Task task2) {

        int priorityComparison = Long.compare(
                task2.getEffectivePriority(),
                task1.getEffectivePriority()
        );

        if (priorityComparison != 0) {
            return priorityComparison;
        }

        int enqueueTimeComparison = Long.compare(
                task1.getEnqueueTime(),
                task2.getEnqueueTime()
        );

        if (enqueueTimeComparison != 0) {
            return enqueueTimeComparison;
        }

        return Long.compare(
                task1.getId(),
                task2.getId()
        );
    }
}