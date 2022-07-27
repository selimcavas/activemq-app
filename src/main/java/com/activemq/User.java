package com.activemq;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class User {
    
    private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    private static String subject = "QUEUE";
    private static Logger LOGGER = Logger.getLogger("User");
    
    public void userWork(int requestCount) throws JMSException, IOException {
        //creating log 
        FileHandler fileHandler = new FileHandler("src/log.log");
        LOGGER.addHandler(fileHandler);
        
        //creating connection to activemq
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        javax.jms.Connection connection = connectionFactory.createConnection();
        connection.start();
         
        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);  
        //setting destination queue to QUEUE
        Destination destination = session.createQueue(subject); 
        
        //creating producer to send messages to QUEUE
        MessageProducer producer = session.createProducer(destination);
         
        //sending messages to QUEUE and logging info
        for (int i = 0; i < requestCount; i++) {
            TextMessage message = SMSCreator(session);
            producer.send(message);
            LOGGER.info((i+1)+ " Added to queue'" + message.getText() + "'");
        }
         
        connection.close();
    }

    public static TextMessage SMSCreator(Session session) throws JMSException{
        
        //4 digit random sms Channel
        int randomSmsChannel = (int) (Math.random() * 9000) + 1000;

        String smsChannel = ""+randomSmsChannel;
        String content = "sms content";
        
        int randomOperator = (int) (Math.random() * 3) + 1;
        String randomNumber = "";
        
        //creating first 3 digits of the number depending on the operator
        if (randomOperator == 1) {
            //Avea
            randomNumber = "505";
        } else if (randomOperator == 2) {
            //Turkcell
            randomNumber = "536";
        } else if (randomOperator == 3) {
            //Vodafone
            randomNumber = "542";
        }

        //creating the rest of the digits
        for (int i = 0; i < 7; i++) {
            randomNumber += (int) (Math.random() * 10);
        }

        String msisdn = randomNumber;

        //creating TextMessage in JSON format
        TextMessage message = session.createTextMessage("{\"smsChannel\":"+"\""+smsChannel+"\"" + 
                                        ",\"msisdn\":" +"\""+ msisdn +"\""+ ",\"content\":" +"\""+ content +"\""+ "}");
        return message;
    }

}
