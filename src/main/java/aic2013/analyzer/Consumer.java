package aic2013.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;
import aic2013.common.entities.Topic;
import aic2013.common.entities.TwitterUser;
import aic2013.common.service.Neo4jService;

public class Consumer implements MessageListener {
	private static Logger logger = Logger.getLogger(Consumer.class.getName());

	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String EXTRACTION_QUEUE_NAME = "tweet-extraction";
	private static final String NEO4J_JDBC_URL = "jdbc:neo4j://localhost:7474";

	private final Neo4jService neo4jService;
	private final ConnectionFactory factory;
	private final Connection connection;
	private final Session session;
	private final MessageConsumer consumer;
	private final TopicExtractor extractor;

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
		String extractionQueueName = getProperty("EXTRACTION_QUEUE_NAME",
				EXTRACTION_QUEUE_NAME);
		String neo4jJdbcUrl = getProperty("NEO4J_JDBC_URL", NEO4J_JDBC_URL);

		Neo4jConnection neo4j = new Driver().connect(neo4jJdbcUrl, new Properties());
        neo4j.setAutoCommit(true);//false);
		Neo4jService neo4jService = new Neo4jService(neo4j);
		
		ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
		Consumer consumer = new Consumer(factory, extractionQueueName, neo4jService);
		consumer.consumer.setMessageListener(consumer);

		System.out
				.println("Started extraction consumer with the following configuration:");
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

	public Consumer(ConnectionFactory factory, String queueName, Neo4jService neo4jService)
			throws JMSException, IOException {
		this.factory = factory;
		connection = factory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		consumer = session.createConsumer(session.createQueue(queueName));
		extractor = new TopicExtractorImpl();
		this.neo4jService = neo4jService;
	}

	public void close() {
		if (consumer != null) {
			try {
				consumer.close();
			} catch (JMSException ex) {
				logger.log(Level.SEVERE, null, ex);
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException ex) {
				logger.log(Level.SEVERE, null, ex);
			}
		}
	}

	public void onMessage(Message message) {
		try {
			if (message instanceof TextMessage) {
				final Status status = DataObjectFactory.createStatus(((TextMessage) message).getText());
				Set<Topic> topics = extractor.extract(status.getText());
				TwitterUser user = new TwitterUser(status.getUser());
				try {
					/* add hash tags to topics */
					for (HashtagEntity tag : status
							.getHashtagEntities()) {
						topics.add(new Topic(new String[]{tag
								.getText()}));
					}
					for (Topic topic : topics) {
						neo4jService
								.createTopicIfAbsent(topic);
						if (status.isRetweet()) {
							neo4jService
									.createRelationIfAbsent(
											"RETWEETS",
											user,
											topic);
						} else {
							neo4jService
									.createRelationIfAbsent(
											"TWEETS",
											user,
											topic);
						}
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			message.acknowledge();
		} catch (JMSException | ExtractionException | TwitterException e) {
			logger.log(Level.SEVERE, null,
					e);
		}
	}

}
