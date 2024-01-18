package ru.yamshikov.mq;

import com.ibm.mq.jms.*;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

public class Main {

    public static void main(String[] args) {

        try {
            MQQueueConnection mqConnection;
            MQQueueConnectionFactory mqConnectionFactory;
            final MQQueueSession mqSession;
            MQQueue mqIn;
            MQQueueReceiver mqReceiver;
            mqConnectionFactory = new MQQueueConnectionFactory();
            mqConnectionFactory.setHostName("localhost");
            mqConnectionFactory.setPort(1414);
            mqConnectionFactory.setQueueManager("TestMQ");
            mqConnectionFactory.setChannel("SYSTEM.DEF.SVRCONN");
            mqConnection = (MQQueueConnection) mqConnectionFactory.createConnection();
            mqSession = (MQQueueSession) mqConnection.createQueueSession(true, MQSession.AUTO_ACKNOWLEDGE);
            mqIn = (MQQueue) mqSession.createQueue("MQ.IN"); //входная очередь
            mqReceiver = (MQQueueReceiver) mqSession.createReceiver(mqIn);

            MQQueueConnection mqConnectionOut;
            MQQueueConnectionFactory mqConnectionFactoryOut;
            final MQQueueSession mqSessionOut;
            mqConnectionFactoryOut = new MQQueueConnectionFactory();
            MQQueue mqOut;
            MQQueueSender mqSender;
            mqConnectionFactoryOut.setHostName("localhost");
            mqConnectionFactoryOut.setPort(1414);
            mqConnectionFactoryOut.setQueueManager("TestMQ");
            mqConnectionFactoryOut.setChannel("SYSTEM.DEF.SVRCONN");
            mqConnectionOut = (MQQueueConnection) mqConnectionFactoryOut.createConnection();
            mqSessionOut = (MQQueueSession) mqConnectionOut.createQueueSession(true, MQSession.AUTO_ACKNOWLEDGE);
            mqOut = (MQQueue) mqSessionOut.createQueue("MQ.OUT"); //выходная очередь
            mqSender = (MQQueueSender) mqSessionOut.createProducer(mqOut);

            MessageListener listener = message -> {
                if (message instanceof TextMessage) {
                    try {
                        System.out.println("Receive in MQ.IN message:");
                        TextMessage textMessage = (TextMessage) message;
                        String text = textMessage.getText();
                        mqSender.send(message);
                        mqSessionOut.commit();
                        System.out.println(text);
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                }
            };

            mqReceiver.setMessageListener(listener);
            mqConnection.start();
            mqConnectionOut.start();
            System.out.println("Start...");

        } catch (JMSException e) {
            System.out.println("Ошибочка: \n");
            e.printStackTrace();
        }
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Program end");


    }


}
