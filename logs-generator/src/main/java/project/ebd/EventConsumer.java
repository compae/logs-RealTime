package project.ebd;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class EventConsumer {

	private static final String USERNAME = "guest";
	private static final String PASSWORD = "guest";
	private final static String QUEUE_NAME = "logsQueue";
	private final static String EXCHANGE_NAME = "logsExchange";
	private final static String ROUTING_KEY = "logsRoute";
	private static final int NUM_BATCH_MESSAGES = 100;
	private int numMessage = 0;
	private ConnectionFactory factory = new ConnectionFactory();
	private Connection connection;
	private Channel channel;

	public EventConsumer(String host, Integer port) throws IOException {

		factory.setUsername(USERNAME);
		factory.setPassword(PASSWORD);
		factory.setVirtualHost("/");
		factory.setHost(host);
		factory.setPort(port);

	}

	public void write(Object[] args) throws IOException {

		EventRegister event = EventRegister.getInstance(args);

		if (numMessage == 0) {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);
			channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
			channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
		}
		// String queueName = channel.queueDeclare().getQueue();
		 
		channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY,
				MessageProperties.PERSISTENT_TEXT_PLAIN, event.JsonOutput().getBytes());

		if(numMessage == NUM_BATCH_MESSAGES){
			channel.close();
			connection.close();
			numMessage = 0;
		} else {
			numMessage++;
		}
	}
}
