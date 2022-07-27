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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Operator {
    
    private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
 
    private static String subject = "QUEUE";
    private static String confirmSubject = "CONFIRMATION_QUEUE";
    private static Logger LOGGER = Logger.getLogger("Operator");
 
    public void operatorWork(int requestCount) throws JMSException, SecurityException, IOException {
        FileHandler fileHandler = new FileHandler("src/log.log");
        LOGGER.addHandler(fileHandler);
        
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        Connection connection = connectionFactory.createConnection();
        connection.start();
 
        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
 
        //two destinations one for recieving messages from QUEUE
        //and one for sending messages to CONFIRMATION_QUEUE
        Destination destination1 = session.createQueue(subject);
        Destination destination2 = session.createQueue(confirmSubject);
 
        MessageConsumer consumer = session.createConsumer(destination1);
        MessageProducer producer = session.createProducer(destination2);
        
        //going to use prettyPrint to make the json more readable
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        
        for (int i = 0; i < requestCount; i++){

            Message message = consumer.receive();
            
            if (message instanceof TextMessage) {
                
                TextMessage textMessage = (TextMessage) message;

                //turn message into json string
                //I'm doing this because I also get \" from the TextMessage, escaping characters doesn't work while getting the message 
                //I seperate the message into three parts, the smsChannel msisdn and content, using regex.
                String smsChannelPart = textMessage.getText().split(",")[0]; 
                String msisdnPart = textMessage.getText().split(",")[1];
                String contentPart = textMessage.getText().split(",")[2];
                //finally these parts are added to create the json formatted srting
                String json = smsChannelPart + "," + msisdnPart + "," + contentPart;


                //I extract values of smsChannel, msisdn and content from the json string
                String smsChannel = smsChannelPart.split(":")[1].replaceAll("\"", "");
                String msisdn = msisdnPart.split(":")[1].replaceAll("\"", "");
                String content = contentPart.split(":")[1].replaceAll("[}\"]", "");

                //I turn json string into json object and pretty print it
                JsonParser parser = new JsonParser();
                JsonObject jsonObject = parser.parse(json).getAsJsonObject();
                String prettyJson = gson.toJson(jsonObject);
                LOGGER.info((i+i)+"\n"+prettyJson);

                int aveaPrice = 100;
                int turkcellPrice = 200;
                int vodafonePrice = 300;
                int operatorcode = Integer.parseInt(msisdn.substring(0, 3));
                int price = 0;

                //calculate price according to operator
                if (operatorcode == 505){
                     price = aveaPrice;
                }
                else if (operatorcode == 536){
                     price = turkcellPrice;
                }
                else if (operatorcode == 542){
                     price = vodafonePrice;
                }

                //check if user is able to pay, send message to CONFIRMATION_QUEUE if he is able to pay
                if(getBalance(msisdn) >= price){
                    producer.send(createConfirmationMessage(session, msisdn));
                    LOGGER.info("Sent confirmation message for "+ msisdn +" with balance of "+ getBalance(msisdn));
                }
                else{
                    LOGGER.info("Not enough balance for "+ msisdn +" with balance of "+ getBalance(msisdn));
                }
            }
        }
        connection.close();
    }
    //method to create confirmation message
    public static TextMessage createConfirmationMessage(Session session, String msisdn){
        TextMessage message;
        LOGGER.info("msisdn: "+ msisdn);
        long msisdnINT = Long.parseLong(msisdn);
        //create status get last digit of msisdn and divide it by two status range is 1-4
        int status = (int) (msisdnINT % 10)/2;
        try {
            message = session.createTextMessage("{\"msisdn\":"+"\""+msisdn+"\"" + ",\"status\":" +"\""+ status +"\""+ "}");
            return message;
        } catch (JMSException e) {
            e.printStackTrace();
            return null;
        }   
    }
    //method to get balance of user, it is set as the last 3 digits of its msisdn
    public static int getBalance(String msisdn){
        int balance = 0;
        long msisdnINT = Long.parseLong(msisdn);
        balance = (int) (msisdnINT % 1000);
        return balance;
    }
    
}
