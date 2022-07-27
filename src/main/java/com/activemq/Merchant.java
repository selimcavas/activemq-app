package com.activemq;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Merchant {
    private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    private static String subject = "CONFIRMATION_QUEUE";
    private static String finalSubject = "RESULTS_QUEUE";
    private static Logger LOGGER = Logger.getLogger("Merchant");
    
    public void merchantWork(int requestCount) throws SecurityException, IOException, JMSException  {
        FileHandler fileHandler = new FileHandler("src/log.log");
        LOGGER.addHandler(fileHandler);
        
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);

        //two destinations one for recieving messages from CONFIRMATION_QUEUE
        //and one for sending messages to RESULTS_QUEUE
        Destination destination1 = session.createQueue(subject);
        Destination destination2 = session.createQueue(finalSubject);
    
        MessageConsumer consumer = session.createConsumer(destination1);
        MessageProducer producer = session.createProducer(destination2);
        

        for (int i = 0; i < requestCount; i++) {
            
            Message message = consumer.receive();

            if (message instanceof TextMessage) {

                TextMessage textMessage = (TextMessage) message;
                String msisdnPart = textMessage.getText().split(",")[0];
                String statusPart = textMessage.getText().split(",")[1];
                //extract msisdn and status
                String msisdn = msisdnPart.split(":")[1].replaceAll("[{\"]", "");
                String status = statusPart.split(":")[1].replaceAll("[\"}]", "");
    
                LOGGER.info((i+1)+" "+ msisdn+ " " + status);
                //depending on the status log a message to the console, only send a message to the RESULTS_QUEUE if the status is 0
                if(status.equals("0")){
                    TextMessage resultMessage = session.createTextMessage("{\"msisdn\":"+"\""+msisdn+"\"" + ",\"status\":" +"\""+ status +"\""+ "}");
                    producer.send(resultMessage);
                    LOGGER.info("Successfully sent result message to results queue.");
                }
                else if (status.equals("1")){
                    LOGGER.info("Connection error!");
                }
                else if (status.equals("2")){
                    LOGGER.info("Server error!");
                }
                else if (status.equals("3")){
                    LOGGER.info("500 error");
                }
                else if (status.equals("4")){
                    LOGGER.info("Unknown error");
                }
            }
        }
        connection.close();      
    }
}
