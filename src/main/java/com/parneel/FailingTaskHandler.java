package com.parneel;

public class FailingTaskHandler implements TaskHandler {

    @Override
    public String execute(String payload) throws Exception {

        throw new RuntimeException(
                "Intentional failure for retry testing"
        );
    }
}