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

public class GenMeanIndexBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;	
	private OutputCollector collector;
	private String esClusterName = "";
	private String esHost = "";
	private Integer esPort;
	private Integer esShards;
	private Integer esNrep;
	private String meanMapLabel;
	private ElasticSearchIndex GenMeanIndex;

	public void execute(Tuple input) {

		String typeMessage = (String) input.getValueByField("meanGenLabel");

		if(!typeMessage.equals("")){
			
			Object obj=JSONValue.parse(input.getValueByField("jsonIn").toString());
			JSONObject jsonObjIn = new JSONObject();		
			jsonObjIn = (JSONObject) obj;

			XContentBuilder builderClient = null;
			try {
				Calendar calendario = GregorianCalendar.getInstance();
				Date fecha = calendario.getTime();

				builderClient = XContentFactory.jsonBuilder().startObject()
						.field("application", "Generic")    
						.field("dateTimeJob", fecha)
						.field("meanCpuUse", Long.parseLong(jsonObjIn.get("cpuUse").toString()) / Long.parseLong(jsonObjIn.get("count").toString()))
						.field("meanMemoryUse", Long.parseLong(jsonObjIn.get("memoryUse").toString()) / Long.parseLong(jsonObjIn.get("count").toString()))
						.field("totalNetworkTraffic", Long.parseLong(jsonObjIn.get("networkTraffic").toString()))
						.endObject();
				
				GenMeanIndex.InsertDocument("", builderClient);
				
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
		
		meanMapLabel = stormConf.get("meanMapLabel").toString();
		
		Calendar calendario = GregorianCalendar.getInstance();
		Date fecha = calendario.getTime();
		SimpleDateFormat formatoDeFecha = new SimpleDateFormat("yyyy.MM.dd");
		
		GenMeanIndex = new ElasticSearchIndex(esClusterName, esHost, esPort, "storm-" + formatoDeFecha.format(fecha), meanMapLabel, esShards, esNrep);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {

	}

	@Override
	public void cleanup() {
		
		GenMeanIndex.Close();
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}