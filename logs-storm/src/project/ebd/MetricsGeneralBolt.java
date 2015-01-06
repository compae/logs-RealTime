package project.ebd;

import java.util.Map;

import org.json.simple.JSONObject;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MetricsGeneralBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;
	private Jedis jedis;	
	private OutputCollector collector;
	private Integer sysErrNum;
	private Integer netErrNum;
	private String genKey = "";
	
	public void execute(Tuple input) {
		
		JSONObject jsonObj = new JSONObject();	
		jsonObj = (JSONObject) input.getValueByField("message");
		
		if((!genKey.equals("")) && (jedis.isConnected()) && (jsonObj.containsKey("cpuUse"))  && (jsonObj.containsKey("memoryUse")) && (jsonObj.containsKey("networkTraffic"))){
		
			Integer cpuUse = Integer.parseInt(jsonObj.get("cpuUse").toString());
			Integer memoryUse = Integer.parseInt(jsonObj.get("memoryUse").toString());
			
			
			jedis.hincrBy(genKey, "count", 1);
			jedis.hincrBy(genKey, "cpuUse", cpuUse);
			jedis.hincrBy(genKey, "memoryUse", memoryUse);
			jedis.hincrBy(genKey, "networkTraffic", Long.parseLong(jsonObj.get("networkTraffic").toString()));
			
			if((!jedis.hexists(genKey, "maxCpuUse")) || (jedis.hget(genKey, "maxCpuUse") == null) || (jedis.hget(genKey, "maxCpuUse").toString().equals(""))){
				jedis.hset(genKey, "maxCpuUse", jsonObj.get("cpuUse").toString());
				jedis.hset(genKey, "maxCpuUseData", jsonObj.toJSONString());
			} else{
				if(jedis.hexists(genKey, "maxCpuUse")){
					if(Integer.parseInt(jedis.hget(genKey, "maxCpuUse")) < cpuUse){
						jedis.hset(genKey, "maxCpuUse", jsonObj.get("cpuUse").toString());
						jedis.hset(genKey, "maxCpuUseData", jsonObj.toJSONString());
					}
				}
			}
			
			if((!jedis.hexists(genKey, "minCpuUse")) || (jedis.hget(genKey, "minCpuUse") == null) || (jedis.hget(genKey, "minCpuUse").toString().equals(""))){
				jedis.hset(genKey, "minCpuUse", jsonObj.get("cpuUse").toString());
				jedis.hset(genKey, "minCpuUseData", jsonObj.toJSONString());
			} else{
				if(jedis.hexists(genKey, "minCpuUse")){
					if(Integer.parseInt(jedis.hget(genKey, "minCpuUse")) > cpuUse){
						jedis.hset(genKey, "minCpuUse", jsonObj.get("cpuUse").toString());
						jedis.hset(genKey, "minCpuUseData", jsonObj.toJSONString());
					}
				}
			}
			
			if((!jedis.hexists(genKey, "maxMemoryUse")) || (jedis.hget(genKey, "maxMemoryUse") == null) || (jedis.hget(genKey, "maxMemoryUse").toString().equals(""))){
				jedis.hset(genKey, "maxMemoryUse", jsonObj.get("memoryUse").toString());
				jedis.hset(genKey, "maxMemoryUseData", jsonObj.toJSONString());
			} else {
				if(jedis.hexists(genKey, "maxMemoryUse")){
					if((Integer.parseInt(jedis.hget(genKey, "maxMemoryUse")) < memoryUse)){
						jedis.hset(genKey, "maxMemoryUse", jsonObj.get("memoryUse").toString());
						jedis.hset(genKey, "maxMemoryUseData", jsonObj.toJSONString());
					}
				}
			}
			
			if((!jedis.hexists(genKey, "minMemoryUse")) || (jedis.hget(genKey, "minMemoryUse") == null) || (jedis.hget(genKey, "minMemoryUse").toString().equals(""))){
				jedis.hset(genKey, "minMemoryUse", jsonObj.get("memoryUse").toString());
				jedis.hset(genKey, "minMemoryUseData", jsonObj.toJSONString());
			} else {
				if(jedis.hexists(genKey, "minMemoryUse")){
					if((Integer.parseInt(jedis.hget(genKey, "minMemoryUse")) > memoryUse)){
						jedis.hset(genKey, "minMemoryUse", jsonObj.get("memoryUse").toString());
						jedis.hset(genKey, "minMemoryUseData", jsonObj.toJSONString());
					}
				}
			}
			
			if(jsonObj.get("systemError").toString().equals("Y")){
				
				jedis.hincrBy("Error-" + genKey, "systemError", 1);
				Long numError = jedis.hincrBy("Error-" + genKey, "systemError " + jsonObj.get("operatingSystem").toString(), 1);
				
				if(jedis.hexists("Error-" + genKey, "systemError " + jsonObj.get("operatingSystem").toString())){
					if(numError == sysErrNum.longValue()){
						jedis.hdel("Error-" + genKey,  "systemError " + jsonObj.get("operatingSystem").toString());
						collector.emit(new Values(jsonObj,"Error-" + genKey + " systemError " + jsonObj.get("operatingSystem").toString(),""));
						//jedis.hset(genKey,  "systemError " + jsonObj.get("operatingSystem").toString(), "0");
					}
				}
			}
			
			if(!jsonObj.get("networkStatus").toString().equals("ONLINE")){
				
				jedis.hincrBy("Error-" + genKey, "networkStatus", 1);
				Long numNetError = jedis.hincrBy("Error-" + genKey, "networkStatus " + jsonObj.get("city").toString(), 1);

				if(jedis.hexists("Error-" + genKey, "networkStatus " + jsonObj.get("city").toString())){
					if(numNetError == netErrNum.longValue()){
						jedis.hdel("Error-" + genKey,  "networkStatus " + jsonObj.get("city").toString());
						collector.emit(new Values(jsonObj,"", "Error-" + genKey + " networkStatus " + jsonObj.get("city").toString()));
						//jedis.hset(genKey,  "networkStatus " + jsonObj.get("city").toString(), "0");
					}
				}
			}
		}

		collector.ack(input);
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		
		this.collector = outputCollector;
		
		String redisHost = stormConf.get("redisHost").toString();
		Integer redisPort = Integer.parseInt(stormConf.get("redisPort").toString());
		connectToRedis(redisHost, redisPort);
		sysErrNum =  Integer.parseInt(stormConf.get("sysErrNum").toString());
		netErrNum =  Integer.parseInt(stormConf.get("netErrNum").toString());
		genKey = stormConf.get("genKey").toString();
		
	}
	
	private void connectToRedis(String host, Integer port) {
		
		jedis = new Jedis(host, port);
		jedis.connect();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		
		outputDeclarer.declare(new Fields("message","systemError","networkStatus"));
		
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
