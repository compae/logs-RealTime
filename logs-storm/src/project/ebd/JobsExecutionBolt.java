package project.ebd;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JobsExecutionBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;	
	private OutputCollector collector;
	private boolean GenMeanIdxCreated;
	private boolean GenMaxMinIdxCreated;
	private boolean AppMeanIdxCreated;
	private boolean AppMaxMinIdxCreated;
	private String genLabel;
	private String appLabel;

	public void execute(Tuple input) {

		Object obj=JSONValue.parse(input.getValueByField("message").toString());
		JSONObject jsonObjIn = new JSONObject();		
		jsonObjIn = (JSONObject) obj;
		String typeMessage = (String) input.getValueByField("type");
		String application = (String) input.getValueByField("typeValue");
		
		if(typeMessage.equals(genLabel.toLowerCase())){
			
			if((GenMeanIdxCreated) && (jsonObjIn.get("cpuUse") != null) && (jsonObjIn.get("memoryUse") != null)
					&& (jsonObjIn.get("count") != null) && (!jsonObjIn.get("count").equals("")) && (!jsonObjIn.get("count").equals("0")) 
					&& (!jsonObjIn.get("memoryUse").equals("0")) && (!jsonObjIn.get("memoryUse").equals(""))
					&& (!jsonObjIn.get("cpuUse").equals(""))&& (!jsonObjIn.get("cpuUse").equals("0")) && (jsonObjIn.get("networkTraffic") != null)) {
				
				JSONObject jsonObj = new JSONObject();		
				collector.emit("streamMeanGenLabel",new Values(jsonObjIn, jsonObj, "mean"+genLabel, application));
				
			}
			
			if((GenMaxMinIdxCreated) && ((jsonObjIn.get("maxCpuUse") != null) || (jsonObjIn.get("minCpuUse") != null)
					|| (jsonObjIn.get("maxMemoryUse") != null)|| (jsonObjIn.get("minMemoryUse") != null))) {
								
				if((jsonObjIn.get("maxCpuUse") != null) && (jsonObjIn.get("maxCpuUseData") != null)
						&& (!jsonObjIn.get("maxCpuUse").equals("")) && (!jsonObjIn.get("maxCpuUse").equals("0"))
						&& (!jsonObjIn.get("maxCpuUseData").equals("")) && (!jsonObjIn.get("maxCpuUseData").equals("0"))){
					
					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxCpuUseData").toString());					
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;
					
					if(jsonObj.size() >= 20){
						
						collector.emit("streamMaxMinGenLabel", new Values(jsonObjIn, jsonObj, "maxCpuUse"+genLabel, application));
						
					}
				}

				if((jsonObjIn.get("minCpuUse") != null) && (jsonObjIn.get("minCpuUseData") != null)
						&& (!jsonObjIn.get("minCpuUse").equals("")) && (!jsonObjIn.get("minCpuUse").equals("0"))
						&& (!jsonObjIn.get("minCpuUseData").equals("")) && (!jsonObjIn.get("minCpuUseData").equals("0"))){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minCpuUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 20){

						collector.emit("streamMaxMinGenLabel", new Values(jsonObjIn, jsonObj, "minCpuUse"+genLabel, application));

					}
				}

				if((jsonObjIn.get("maxMemoryUse") != null) && (jsonObjIn.get("maxMemoryUseData") != null)
						&& (!jsonObjIn.get("maxMemoryUse").equals("")) && (!jsonObjIn.get("maxMemoryUse").equals("0"))
						&& (!jsonObjIn.get("maxMemoryUseData").equals("")) && (!jsonObjIn.get("maxMemoryUseData").equals("0"))){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxMemoryUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 20){

						collector.emit("streamMaxMinGenLabel", new Values(jsonObjIn, jsonObj, "maxMemoryUse"+genLabel, application));

					}
				}

				if((jsonObjIn.get("minMemoryUse") != null) && (jsonObjIn.get("minMemoryUseData") != null)
						&& (!jsonObjIn.get("minMemoryUse").equals("")) && (!jsonObjIn.get("minMemoryUse").equals("0"))
						&& (!jsonObjIn.get("minMemoryUseData").equals("")) && (!jsonObjIn.get("minMemoryUseData").equals("0"))){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minMemoryUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 20){

						collector.emit("streamMaxMinGenLabel", new Values(jsonObjIn, jsonObj, "minMemoryUse"+genLabel, application));

					}
				}
			}
		}

		if(typeMessage.equals(appLabel.toLowerCase())){
			
			if((AppMeanIdxCreated) && (jsonObjIn.get("cpuUse") != null) && (jsonObjIn.get("memoryUse") != null)
					&& (jsonObjIn.get("count") != null) && (!jsonObjIn.get("count").equals("")) && (!jsonObjIn.get("count").equals("0")) 
					&& (!jsonObjIn.get("memoryUse").equals("0")) && (!jsonObjIn.get("memoryUse").equals(""))
					&& (!jsonObjIn.get("cpuUse").equals("")) && (!jsonObjIn.get("cpuUse").equals("0")) && (jsonObjIn.get("networkTraffic") != null)) {
				
				JSONObject jsonObj = new JSONObject();
				collector.emit("streamMeanAppLabel", new Values(jsonObjIn, jsonObj, "mean"+appLabel, application));
				
			}
			
			if((AppMaxMinIdxCreated) && ((jsonObjIn.get("maxCpuUse") != null) || (jsonObjIn.get("minCpuUse") != null)
					|| (jsonObjIn.get("maxMemoryUse") != null)|| (jsonObjIn.get("minMemoryUse") != null))) {
				
				if((jsonObjIn.get("maxCpuUse") != null) && (jsonObjIn.get("maxCpuUseData") != null)
						&& (!jsonObjIn.get("maxCpuUse").equals("")) && (!jsonObjIn.get("maxCpuUse").equals("0"))
						&& (!jsonObjIn.get("maxCpuUseData").equals("")) && (!jsonObjIn.get("maxCpuUseData").equals("0"))){
					
					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxCpuUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;
					
					if(jsonObj.size() >= 20){
						
						collector.emit("streamMaxMinAppLabel", new Values(jsonObjIn, jsonObj, "maxCpuUse"+appLabel, application));
						
					}
				}

				if((jsonObjIn.get("minCpuUse") != null) && (jsonObjIn.get("minCpuUseData") != null)
						&& (!jsonObjIn.get("minCpuUse").equals("")) && (!jsonObjIn.get("minCpuUse").equals("0"))
						&& (!jsonObjIn.get("minCpuUseData").equals("")) && (!jsonObjIn.get("minCpuUseData").equals("0"))){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minCpuUseData").toString());					
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 20){

						collector.emit("streamMaxMinAppLabel", new Values(jsonObjIn, jsonObj, "minCpuUse"+appLabel, application));

					}
				}

				if((jsonObjIn.get("maxMemoryUse") != null) && (jsonObjIn.get("maxMemoryUseData") != null)
						&& (!jsonObjIn.get("maxMemoryUse").equals("")) && (!jsonObjIn.get("maxMemoryUse").equals("0"))
						&& (!jsonObjIn.get("maxMemoryUseData").equals("")) && (!jsonObjIn.get("maxMemoryUseData").equals("0"))){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxMemoryUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 20){

						collector.emit("streamMaxMinAppLabel", new Values(jsonObjIn, jsonObj, "maxMemoryUse"+appLabel, application));

					}
				}

				if((jsonObjIn.get("minMemoryUse") != null) && (jsonObjIn.get("minMemoryUseData") != null)
						&& (!jsonObjIn.get("minMemoryUse").equals("")) && (!jsonObjIn.get("minMemoryUse").equals("0"))
						&& (!jsonObjIn.get("minMemoryUseData").equals("")) && (!jsonObjIn.get("minMemoryUseData").equals("0"))){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minMemoryUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 20){

						collector.emit("streamMaxMinAppLabel", new Values(jsonObjIn, jsonObj, "minMemoryUse"+appLabel, application));

					}
				}
				
			}
		}
		
		collector.ack(input);
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		
		this.collector = outputCollector;
				
		genLabel = stormConf.get("genLabel").toString();
		appLabel = stormConf.get("appLabel").toString();
		GenMeanIdxCreated  = (boolean) Boolean.valueOf(stormConf.get("GenMeanIdxCreated").toString());
		GenMaxMinIdxCreated  = (boolean) Boolean.valueOf(stormConf.get("GenMaxMinIdxCreated").toString());
		AppMeanIdxCreated  = (boolean) Boolean.valueOf(stormConf.get("AppMeanIdxCreated").toString());
		AppMaxMinIdxCreated  = (boolean) Boolean.valueOf(stormConf.get("AppMaxMinIdxCreated").toString());
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		
		//outputDeclarer.declare(new Fields("jsonIn", "jsonData", "meanGenLabel", "maxMinGenLabel","meanAppLabel", "maxMinAppLabel", "application"));
		outputDeclarer.declareStream("streamMeanGenLabel", new Fields("jsonIn", "jsonData", "meanGenLabel", "application"));
		outputDeclarer.declareStream("streamMaxMinGenLabel", new Fields("jsonIn", "jsonData", "maxMinGenLabel", "application"));
		outputDeclarer.declareStream("streamMeanAppLabel", new Fields("jsonIn", "jsonData", "meanAppLabel", "application"));
		outputDeclarer.declareStream("streamMaxMinAppLabel", new Fields("jsonIn", "jsonData", "maxMinAppLabel", "application"));
		
		
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
