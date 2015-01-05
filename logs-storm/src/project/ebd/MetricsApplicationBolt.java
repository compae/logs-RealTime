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

public class MetricsApplicationBolt implements IRichBolt{
	private static final long serialVersionUID = 1L;
	private Jedis jedis;	
	private OutputCollector collector;
	private Integer sysErrNum;
	private Integer netErrNum;
	
	public void execute(Tuple input) {
		
		JSONObject jsonObj = new JSONObject();		
		jsonObj = (JSONObject) input.getValueByField("message");
		String application = jsonObj.get("application").toString();
		Integer cpuUse = Integer.parseInt(jsonObj.get("cpuUse").toString());
		Integer memoryUse = Integer.parseInt(jsonObj.get("memoryUse").toString());
		
		jedis.hincrBy(application, "count", 1);
		jedis.hincrBy(application, "cpuUse", cpuUse);
		jedis.hincrBy(application, "memoryUse", memoryUse);
		jedis.hincrBy(application, "networkTraffic", Long.parseLong(jsonObj.get("networkTraffic").toString()));
			
		if((!jedis.hexists(application, "maxCpuUse")) || (jedis.hget(application, "maxCpuUse") == null) || (jedis.hget(application, "maxCpuUse").toString().equals(""))){
			jedis.hset(application, "maxCpuUse", jsonObj.get("cpuUse").toString());
			jedis.hset(application, "maxCpuUseData", jsonObj.toJSONString());
		} else{
			if(jedis.hexists(application, "maxCpuUse")){
				if(Integer.parseInt(jedis.hget(application, "maxCpuUse")) < cpuUse){
					jedis.hset(application, "maxCpuUse", jsonObj.get("cpuUse").toString());
					jedis.hset(application, "maxCpuUseData", jsonObj.toJSONString());
				}
			}
		}
		
		if((!jedis.hexists(application, "minCpuUse")) || (jedis.hget(application, "minCpuUse") == null) || (jedis.hget(application, "minCpuUse").toString().equals(""))){
			jedis.hset(application, "minCpuUse", jsonObj.get("cpuUse").toString());
			jedis.hset(application, "minCpuUseData", jsonObj.toJSONString());
		} else{
			if(jedis.hexists(application, "minCpuUse")){
				if(Integer.parseInt(jedis.hget(application, "minCpuUse")) > cpuUse){
					jedis.hset(application, "minCpuUse", jsonObj.get("cpuUse").toString());
					jedis.hset(application, "minCpuUseData", jsonObj.toJSONString());
				}
			}
		}
		
		if((!jedis.hexists(application, "maxMemoryUse")) || (jedis.hget(application, "maxMemoryUse") == null) || (jedis.hget(application, "maxMemoryUse").toString().equals(""))){
			jedis.hset(application, "maxMemoryUse", jsonObj.get("memoryUse").toString());
			jedis.hset(application, "maxMemoryUseData", jsonObj.toJSONString());
		} else {
			if(jedis.hexists(application, "maxMemoryUse")){
				if((Integer.parseInt(jedis.hget(application, "maxMemoryUse")) < memoryUse)){
					jedis.hset(application, "maxMemoryUse", jsonObj.get("memoryUse").toString());
					jedis.hset(application, "maxMemoryUseData", jsonObj.toJSONString());
				}
			}
		}
		
		if((!jedis.hexists(application, "minMemoryUse")) || (jedis.hget(application, "minMemoryUse") == null) || (jedis.hget(application, "minMemoryUse").toString().equals(""))){
			jedis.hset(application, "minMemoryUse", jsonObj.get("memoryUse").toString());
			jedis.hset(application, "minMemoryUseData", jsonObj.toJSONString());
		} else {
			if(jedis.hexists(application, "minMemoryUse")){
				if((Integer.parseInt(jedis.hget(application, "minMemoryUse")) > memoryUse)){
					jedis.hset(application, "minMemoryUse", jsonObj.get("memoryUse").toString());
					jedis.hset(application, "minMemoryUseData", jsonObj.toJSONString());
				}
			}
		}
		
		if(jsonObj.get("systemError").toString().equals("Y")){
			
			jedis.hincrBy("Error-" + application, "systemError", 1);
			Long numError = jedis.hincrBy("Error-" + application, "systemError " + jsonObj.get("operatingSystem").toString(), 1);
			
			if(jedis.hexists("Error-" + application, "systemError " + jsonObj.get("operatingSystem").toString())){
				if(numError == sysErrNum.longValue()){
					jedis.hdel("Error-" + application,  "systemError " + jsonObj.get("operatingSystem").toString());
					collector.emit(new Values(jsonObj,"Error-" + application + " systemError " + jsonObj.get("operatingSystem").toString(),""));
					//jedis.hset(application,  "systemError " + jsonObj.get("operatingSystem").toString(), "0");
				}
			}
		}
		
		if(!jsonObj.get("networkStatus").toString().equals("ONLINE")){
			
			jedis.hincrBy("Error-" +  application, "networkStatus", 1);
			Long numNetError = jedis.hincrBy("Error-" + application, "networkStatus " + jsonObj.get("city").toString(), 1);
			
			if(jedis.hexists("Error-" + application, "networkStatus " + jsonObj.get("city").toString())){
				if(numNetError == netErrNum.longValue()){
					jedis.hdel("Error-" + application,  "networkStatus " + jsonObj.get("city").toString());
					collector.emit(new Values(jsonObj,"","Error-" +  application + " networkStatus " + jsonObj.get("city").toString()));
					//jedis.hset(application,  "networkStatus " + jsonObj.get("city").toString(), "0");
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
