package com.parneel;

import java.util.HashMap;
import java.util.Map;

public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry() {
        handlers = new HashMap<>();
    }

    public void register(
            String taskType,
            TaskHandler handler
    ) {
        handlers.put(taskType, handler);
    }

    public TaskHandler getHandler(String taskType) {
        return handlers.get(taskType);
    }
}