package com.activemq;

import java.io.IOException;

import javax.jms.JMSException;

public class Test {
    public static void main(String[] args) throws JMSException, InterruptedException, IOException {
        User u = new User();
        Operator op = new Operator();
        Merchant m = new Merchant();
        
        int requestCount = 100;

        u.userWork(requestCount);
        Thread.sleep(10000);
        op.operatorWork(requestCount);
        Thread.sleep(10000); 
        m.merchantWork(requestCount);
    }
}
