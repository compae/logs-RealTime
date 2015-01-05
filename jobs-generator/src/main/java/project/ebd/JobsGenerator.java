package project.ebd;

import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class JobsGenerator {
	
	private static final String HOST = "localhost";
	private static final Integer PORT = 5672;
	private static final String USERNAME = "guest";
	private static final String PASSWORD = "guest";
	private static final String RABBITMQ_QUEUE_JOBS = "jobsQueue";
	private static final String RABBITMQ_EXCHANGE_JOBS = "jobsExchange";
	private static final String RABBITMQ_ROUTING_KEY_JOBS = "jobsRoute";

	private static ConnectionFactory factory = new ConnectionFactory();
	private static Connection connection;
	private static Channel channel;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		if((args.length == 4) && (Integer.parseInt(args[0]) > 0) && (Integer.parseInt(args[3]) > 0)){
		
			Integer numJobs = Integer.parseInt(args[0]);
			String genKey = args[1];
			String appKeys = args[2];
			String[] appKeysArray = appKeys.split(",");
			Integer timeJobs = Integer.parseInt(args[3]);
			
			factory.setUsername(USERNAME);
			factory.setPassword(PASSWORD);
			factory.setVirtualHost("/");
			factory.setHost(HOST);
			factory.setPort(PORT);
		
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(RABBITMQ_QUEUE_JOBS, true, false, false, null);
			channel.exchangeDeclare(RABBITMQ_EXCHANGE_JOBS, "direct", true);
			channel.queueBind(RABBITMQ_QUEUE_JOBS, RABBITMQ_EXCHANGE_JOBS, RABBITMQ_ROUTING_KEY_JOBS);
		
			for(int i = 0 ; i < numJobs ; i++){
				
				Gson gson = new Gson();
				
				if(!genKey.equals("none")){
					JsonObject genObject = new JsonObject();
					genObject.addProperty("genKey", genKey);
					channel.basicPublish(RABBITMQ_EXCHANGE_JOBS, RABBITMQ_ROUTING_KEY_JOBS,
							MessageProperties.PERSISTENT_TEXT_PLAIN, gson.toJson(genObject).getBytes());
				}
				
				if(!appKeys.equals("none")){
					for(String appKey : appKeysArray){
						JsonObject appObject = new JsonObject();
						appObject.addProperty("appKey", appKey);
						channel.basicPublish(RABBITMQ_EXCHANGE_JOBS, RABBITMQ_ROUTING_KEY_JOBS,
								MessageProperties.PERSISTENT_TEXT_PLAIN, gson.toJson(appObject).getBytes());
					}
				}

				Thread.sleep(timeJobs);
			}
			
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}

	}

}
