package project.ebd;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ApplicationBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;	
	String applicationConf = "";
	String[] applicationConfArray;
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
				
		for(String appConf : applicationConfArray){
			if(appConf.equals(jsonObj.get("application").toString())){
				collector.emit(new Values(jsonObj));
			}
		}
		
		collector.ack(input);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		
		this.collector = outputCollector;
		
		applicationConf = stormConf.get("applications").toString();
		applicationConfArray = applicationConf.split(",");
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		
		outputDeclarer.declare(new Fields("message"));
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
