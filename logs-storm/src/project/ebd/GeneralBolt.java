package project.ebd;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GeneralBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;
	private Jedis jedis;	
	Map<String,Integer> results = new HashMap<String,Integer>();
	private OutputCollector collector;
	
	public void execute(Tuple input) {

		String message = input.getStringByField("log");
		
		JSONObject jsonObj = new JSONObject();
		JSONParser parser = new JSONParser();
		
		try {
			Object messageObject= parser.parse(message);
			jsonObj = (JSONObject) messageObject;
		} catch (ParseException e) {
			e.printStackTrace();
		}
				
		jedis.incr("totalLogs");
		
		collector.emit(new Values(jsonObj));
		
		collector.ack(input);
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		
		this.collector = outputCollector;
		
		String redisHost = stormConf.get("redisHost").toString();
		Integer redisPort = Integer.parseInt(stormConf.get("redisPort").toString());
		connectToRedis(redisHost, redisPort);
		
	}
	
	private void connectToRedis(String host, Integer port) {
		
		jedis = new Jedis(host, port);
		jedis.connect();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		
		outputDeclarer.declare(new Fields("message"));
		
	}

	@Override
	public void cleanup() {
		
		jedis.close();	
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
