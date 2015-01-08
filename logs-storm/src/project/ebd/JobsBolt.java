package project.ebd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;

public class JobsBolt implements IRichBolt{
	
	private static final long serialVersionUID = 1L;
	private Jedis jedis;	
	private OutputCollector collector;
	
	public void execute(Tuple input) {
		
		JSONObject jsonObjIn = new JSONObject();		
		jsonObjIn = (JSONObject) input.getValueByField("job");
		String valueKey = "";
		String typeKey = "";
		
		if(jsonObjIn.containsKey("genKey")){
			valueKey = (String) jsonObjIn.get("genKey");
			typeKey = "genkey";
		} else {
			if(jsonObjIn.containsKey("appKey")){
				valueKey = (String) jsonObjIn.get("appKey");
				typeKey = "appkey";
			}
		}
		
		if(!valueKey.equals("") && !typeKey.equals("")){
				
			Map<String, String> hValues = new HashMap<String, String>();
			Gson gson = new Gson();
			
			jedis.incr("totalJobs");
			hValues = jedis.hgetAll(valueKey);	
			//jedis.del(valueKey);
			
			Set<String> hKeysToDel = jedis.hkeys(valueKey);
			Iterator<String> it = hKeysToDel.iterator();
			while(it.hasNext()){
				String key = it.next();
				if(key.contains("Data")){
					jedis.hset(valueKey, key, "");
				} else {
					jedis.hset(valueKey, key, "0");
				}
			}
			
			collector.emit(new Values(gson.toJson(hValues), typeKey, valueKey));

		}
		
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
		
		outputDeclarer.declare(new Fields("message","type", "typeValue"));
		
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
