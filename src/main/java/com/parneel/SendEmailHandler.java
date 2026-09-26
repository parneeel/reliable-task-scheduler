package com.parneel;

public class SendEmailHandler implements TaskHandler {

    @Override
    public String execute(String payload) throws Exception {
        System.out.println("SENDING EMAIL TO: " + payload);
        Thread.sleep(100);
        System.out.println("EMAIL SENT TO: " + payload);
        return payload;
    }
}