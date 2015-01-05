package project.ebd;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class GenMaxMinIndexBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;	
	private OutputCollector collector;
	private String esClusterName = "";
	private String esHost = "";
	private Integer esPort;
	private Integer esShards;
	private Integer esNrep;
	private String genLabel;
	private String maxMinMapLabel;
	private ElasticSearchIndex GenMaxMinIndex;

	public void execute(Tuple input) {

		String typeMessage = (String) input.getValueByField("maxMinGenLabel");

		if(!typeMessage.equals("")){

			Object obj=JSONValue.parse(input.getValueByField("jsonIn").toString());
			JSONObject jsonObjIn = new JSONObject();		
			jsonObjIn = (JSONObject) obj;

			Object dataObj=JSONValue.parse(input.getValueByField("jsonData").toString());
			JSONObject jsonObjData = new JSONObject();		
			jsonObjData = (JSONObject) dataObj;
			
			Integer maxCpuUse = 0;
			if(typeMessage.contains("maxCpuUse"))
				maxCpuUse = Integer.parseInt(jsonObjIn.get("maxCpuUse").toString());
			
			Integer minCpuUse = 0;
			if(typeMessage.contains("minCpuUse"))
				minCpuUse = Integer.parseInt(jsonObjIn.get("minCpuUse").toString());
			
			Integer maxMemoryUse = 0;
			if(typeMessage.contains("maxMemoryUse"))
				maxMemoryUse = Integer.parseInt(jsonObjIn.get("maxMemoryUse").toString());
			
			Integer minMemoryUse = 0;
			if(typeMessage.contains("minMemoryUse"))
				maxCpuUse = Integer.parseInt(jsonObjIn.get("minMemoryUse").toString());

			Calendar calendario = GregorianCalendar.getInstance();
			Date fecha = calendario.getTime();

			XContentBuilder builderClient = null;
			try {
				builderClient = XContentFactory.jsonBuilder().startObject()
						.field("application", "")    
						.field("dateTimeJob", fecha)
						.field("maxCpuUse", maxCpuUse)
						.field("minCpuUse", minCpuUse)
						.field("maxMemoryUse", maxMemoryUse)
						.field("minMemoryUse", minMemoryUse)
						.field("operatingSystem", jsonObjData.get("operatingSystem").toString())
						.field("city", jsonObjData.get("city").toString())
						.field("country", jsonObjData.get("country").toString())
						.field("cpuUse", jsonObjData.get("cpuUse").toString())
						.field("networkStatus", jsonObjData.get("networkStatus").toString())
						.field("systemError", jsonObjData.get("systemError").toString())
						.field("dateTimeEvent", jsonObjData.get("dateTimeEvent").toString())
						.field("dateTimeProcessed", jsonObjData.get("dateTimeProcessed").toString())
						.field("user", jsonObjData.get("user").toString())
						.field("company", jsonObjData.get("company").toString())
						.field("ipServer", jsonObjData.get("ipServer").toString())
						.field("ipHost", jsonObjData.get("ipHost").toString())
						.field("networkTraffic", Long.parseLong(jsonObjData.get("networkTraffic").toString()))
						.endObject();
				
				GenMaxMinIndex.InsertDocument("", builderClient);
				
			} catch (NumberFormatException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		collector.ack(input);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector outputCollector) {
		
		this.collector = outputCollector;
		
		esClusterName = stormConf.get("esClusterName").toString();
		esHost = stormConf.get("esHost").toString();
		esPort = Integer.parseInt(stormConf.get("esPort").toString());
		esShards = Integer.parseInt(stormConf.get("esShards").toString());
		esNrep = Integer.parseInt(stormConf.get("esNrep").toString());
		
		genLabel = stormConf.get("genLabel").toString();
		maxMinMapLabel = stormConf.get("maxMinMapLabel").toString();
		
		Calendar calendario = GregorianCalendar.getInstance();
		Date fecha = calendario.getTime();
		SimpleDateFormat formatoDeFecha = new SimpleDateFormat("yyyy.MM.dd");
		
		GenMaxMinIndex = new ElasticSearchIndex(esClusterName, esHost, esPort, "storm-" + genLabel + "-" + formatoDeFecha.format(fecha), maxMinMapLabel, esShards, esNrep);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {

	}

	@Override
	public void cleanup() {

		GenMaxMinIndex.Close();
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}