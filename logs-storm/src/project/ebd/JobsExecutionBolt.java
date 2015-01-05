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
		
		if(typeMessage.equals(genLabel)){
			
			if((GenMeanIdxCreated) && (jsonObjIn.get("cpuUse") != null) && (jsonObjIn.get("memoryUse") != null) && (jsonObjIn.get("count") != null)) {
				
				JSONObject jsonObj = new JSONObject();		
				collector.emit(new Values(jsonObjIn, jsonObj, "mean"+genLabel, "","","", application));
				
			}
			
			if((GenMaxMinIdxCreated) && ((jsonObjIn.get("maxCpuUse") != null) || (jsonObjIn.get("minCpuUse") != null) || (jsonObjIn.get("maxMemoryUse") != null)|| (jsonObjIn.get("minMemoryUse") != null))) {
								
				if((jsonObjIn.get("maxCpuUse") != null)){
					
					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxCpuUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;
					
					if(jsonObj.size() >= 16){
						
						collector.emit(new Values(jsonObjIn, jsonObj, "", "maxCpuUse"+genLabel,"","", application));
						
					}
				}

				if((jsonObjIn.get("minCpuUse") != null)){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minCpuUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 16){

						collector.emit(new Values(jsonObjIn, jsonObj, "", "minCpuUse"+genLabel,"","", application));

					}
				}

				if((jsonObjIn.get("maxMemoryUse") != null)){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxMemoryUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 16){

						collector.emit(new Values(jsonObjIn, jsonObj, "", "maxMemoryUse"+genLabel,"","",application));

					}
				}

				if((jsonObjIn.get("minMemoryUse") != null)){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minMemoryUseData").toString());
					JSONObject jsonObj = new JSONObject();		
					jsonObj = (JSONObject) maxCpuUseObj;

					if(jsonObj.size() >= 16){

						collector.emit(new Values(jsonObjIn, jsonObj, "", "minMemoryUse"+genLabel,"","",application));

					}
				}
			}
		}

		if(typeMessage.equals("appKey")){
			
			if((AppMeanIdxCreated) && (jsonObjIn.get("cpuUse") != null) && (jsonObjIn.get("memoryUse") != null) && (jsonObjIn.get("count") != null)) {
				
				JSONObject jsonObj = new JSONObject();
				collector.emit(new Values(jsonObjIn, jsonObj, "", "", "mean"+appLabel,"", application));
				
			}
			
			if((AppMaxMinIdxCreated) && (jsonObjIn.get("cpuUse") != null) && (jsonObjIn.get("memoryUse") != null) && (jsonObjIn.get("count") != null)) {
				
				if((jsonObjIn.get("maxCpuUse") != null)){
					
					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxCpuUseData").toString());
					JSONObject jsonObjmax = new JSONObject();		
					jsonObjmax = (JSONObject) maxCpuUseObj;
					
					if(jsonObjmax.size() >= 16){
						
						collector.emit(new Values(jsonObjIn, jsonObjmax, "", "","","maxCpuUse"+genLabel, application));
						
					}
				}

				if((jsonObjIn.get("minCpuUse") != null)){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minCpuUseData").toString());
					JSONObject jsonObjmax = new JSONObject();		
					jsonObjmax = (JSONObject) maxCpuUseObj;

					if(jsonObjmax.size() >= 16){

						collector.emit(new Values(jsonObjIn, jsonObjmax, "", "","","minCpuUse"+genLabel, application));

					}
				}

				if((jsonObjIn.get("maxMemoryUse") != null)){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("maxMemoryUseData").toString());
					JSONObject jsonObjmax = new JSONObject();		
					jsonObjmax = (JSONObject) maxCpuUseObj;

					if(jsonObjmax.size() >= 16){

						collector.emit(new Values(jsonObjIn, jsonObjmax, "", "","","maxMemoryUse"+genLabel, application));

					}
				}

				if((jsonObjIn.get("minMemoryUse") != null)){

					Object maxCpuUseObj=JSONValue.parse(jsonObjIn.get("minMemoryUseData").toString());
					JSONObject jsonObjmax = new JSONObject();		
					jsonObjmax = (JSONObject) maxCpuUseObj;

					if(jsonObjmax.size() >= 16){

						collector.emit(new Values(jsonObjIn, jsonObjmax, "", "","","minMemoryUse"+genLabel, application));

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
		
		outputDeclarer.declare(new Fields("jsonIn", "jsonData", "meanGenLabel", "maxMinGenLabel","meanAppLabel", "maxMinAppLabel", "application"));
		
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
