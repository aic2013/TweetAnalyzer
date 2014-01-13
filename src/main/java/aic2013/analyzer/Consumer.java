package aic2013.analyzer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer implements MessageListener {

    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String EXTRACTION_QUEUE_NAME = "tweet-extraction";

    private final ConnectionFactory factory;
    private final Connection connection;
    private final Session session;
    private final MessageConsumer consumer;

    private static String getProperty(String name, String defaultValue) {
        String value = System.getProperty(name);

        if (value == null) {
            value = System.getenv(name);
        }
        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    public static void main(String[] args) throws Exception {
        String brokerUrl = getProperty("BROKER_URL", BROKER_URL);
        String extractionQueueName = getProperty("EXTRACTION_QUEUE_NAME", EXTRACTION_QUEUE_NAME);

        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Consumer consumer = new Consumer(factory, extractionQueueName);
        consumer.consumer.setMessageListener(consumer);

        System.out.println("Started extraction consumer with the following configuration:");
        System.out.println("\tBroker: " + brokerUrl);
        System.out.println("\t\tExtraction queue name: " + extractionQueueName);
        System.out.println();
        System.out.println("To shutdown the application please type 'exit'.");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String command;

        while ((command = br.readLine()) != null) {
            if ("exit".equals(command)) {
                break;
            }
        }

        consumer.close();
    }

    public Consumer(ConnectionFactory factory, String queueName) throws JMSException {
        this.factory = factory;
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createConsumer(session.createQueue(queueName));
    }

    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException ex) {
                Logger.getLogger(Consumer.class.getName())
                    .log(Level.SEVERE, null, ex);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException ex) {
                Logger.getLogger(Consumer.class.getName())
                    .log(Level.SEVERE, null, ex);
            }
        }
    }

    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                String txtMessage = ((TextMessage) message).getText();
                // TODO: Do extraction
            }
        } catch (JMSException ex) {
            Logger.getLogger(Consumer.class.getName())
                .log(Level.SEVERE, null, ex);
        }
    }
}
