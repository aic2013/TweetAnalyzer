package aic2013.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;
import aic2013.common.entities.Topic;
import aic2013.common.entities.TwitterUser;
import aic2013.common.service.Neo4jService;

public class TweetConsumer {
	private static Logger logger = Logger.getLogger(Consumer.class.getName());

	private static final String BROKER_URL = "amqp://guest:guest@localhost:5672/";
	private static final String EXTRACTION_QUEUE_NAME = "tweet-extraction";
	private static final String NEO4J_JDBC_URL = "jdbc:neo4j://localhost:7474";

	private final Neo4jService neo4jService;
	private final ConnectionFactory factory;
	private final Connection connection;
	private final Channel channel;
	private final Consumer consumer;
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
		String extractionQueueName = getProperty("EXTRACTION_QUEUE_NAME", EXTRACTION_QUEUE_NAME);
		String neo4jJdbcUrl = getProperty("NEO4J_JDBC_URL", NEO4J_JDBC_URL);

		Neo4jConnection neo4j = new Driver().connect(neo4jJdbcUrl, new Properties());
		neo4j.setAutoCommit(true);
		Neo4jService neo4jService = new Neo4jService(neo4j);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(brokerUrl);
		final TweetConsumer consumer = new TweetConsumer(factory, extractionQueueName, neo4jService);

    Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
            if (consumer != null) {
                consumer.close();
            }

            System.out.println("Exiting");
            System.exit(0);
        }
    });

		System.out.println("Started extraction consumer with the following configuration:");
		System.out.println("\tBroker: " + brokerUrl);
		System.out.println("\t\tExtraction queue name: " + extractionQueueName);

    while (true) {
        Thread.sleep(1000);
    }
	}

	public TweetConsumer(ConnectionFactory factory, String queueName, Neo4jService neo4jService)
			throws IOException {
		extractor = new TopicExtractorImpl();
		this.neo4jService = neo4jService;
		this.factory = factory;
		connection = factory.newConnection();
		channel = connection.createChannel();
    channel.queueDeclare(queueName, true, false, false, null);
    channel.basicQos(5);
		consumer = createMessageConsumer(channel);
    channel.basicConsume(queueName, false, consumer);
	}

	public void close() {
		if (channel != null) {
			try {
				channel.close();
			} catch (IOException ex) {
				logger.log(Level.SEVERE, null, ex);
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException ex) {
				logger.log(Level.SEVERE, null, ex);
			}
		}
	}

	private DefaultConsumer createMessageConsumer(Channel aChannel) {
		return new DefaultConsumer(aChannel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				try {
					String message = new String(body);

					final Status status = DataObjectFactory.createStatus(message);
					Set<Topic> topics = extractor.extract(status.getText());
					TwitterUser user = new TwitterUser(status.getUser());

					try {
						/* add hash tags to topics */
						for (HashtagEntity tag : status.getHashtagEntities()) {
							topics.add(new Topic(new String[]{ tag.getText() }));
						}

						for (Topic topic : topics) {
							neo4jService.createTopicIfAbsent(topic);

							if (status.isRetweet()) {
								neo4jService.createRelationIfAbsent("RETWEETS", user, topic);
							} else {
								neo4jService.createRelationIfAbsent("TWEETS", user, topic);
							}
						}
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
					channel.basicAck(envelope.getDeliveryTag(), false);
				} catch (ExtractionException | TwitterException e) {
					logger.log(Level.SEVERE, null, e);
				}
			}
		};
	}
}
