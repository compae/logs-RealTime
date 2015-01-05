package project.ebd;

import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;



public class LogsSpout implements IRichSpout {
	
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		this.factory = new ConnectionFactory();
		
		factory.setUsername(conf.get("rabbitUserName").toString());
		factory.setPassword(conf.get("rabbitPassword").toString());
		factory.setVirtualHost("/");
		factory.setHost(conf.get("rabbitHost").toString());
		factory.setPort( Integer.parseInt(conf.get("rabbitPort").toString()));
		try {
			connection = factory.newConnection();
			channel=connection.createChannel();
			consumer = new QueueingConsumer (channel);
			channel.queueDeclare(conf.get("stormQueue").toString(), true, false, false, null);
			channel.exchangeDeclare(conf.get("stormExchange").toString(), "direct", true);
			channel.queueBind(conf.get("stormQueue").toString(), conf.get("stormExchange").toString(), conf.get("stormRoute").toString());
			channel.basicConsume(conf.get("stormQueue").toString(), true, consumer);
		} catch (IOException e) {

			e.printStackTrace();
		}
		this.collector = collector;
		
	}
 
	@Override
	public void nextTuple() {
	
		try {
			QueueingConsumer.Delivery delivery;
			while ((delivery = consumer.nextDelivery()) != null) {
				String message = new String(delivery.getBody());
				this.collector.emit(new Values(message));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading from rabbitMQ storm queue", e);
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("log"));
		
	}
 
	@Override
	public void close() {
		
		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
	
	public boolean isDistributed() {
		return false;
	}
	
	@Override
	public void activate() {
	}
	@Override
	public void deactivate() {
	}
	@Override
	public void ack(Object msgId) {
	}
	@Override
	public void fail(Object msgId) {
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}