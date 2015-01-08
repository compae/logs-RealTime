package project.ebd;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class IndexesMappings {


	public static boolean GenMeanCreatedIndexElasticSearch(String esClusterName, String esHost, Integer esPort, Integer esShards, Integer esNrep, String meanMapLabel, String maxMinMapLabel){

		Calendar calendario = GregorianCalendar.getInstance();
		Date fecha = calendario.getTime();
		SimpleDateFormat formatoDeFecha = new SimpleDateFormat("yyyy.MM.dd");

		boolean result = false; 

		ElasticSearchIndex GenMeanIndex = new ElasticSearchIndex(esClusterName, esHost, esPort, "storm-" + formatoDeFecha.format(fecha), meanMapLabel, esShards, esNrep);

		if(!GenMeanIndex.IndexExists()){
			result = GenMeanIndex.CreateIndex();

			if(result){
				XContentBuilder meanMapping = null;
				try {
					meanMapping = XContentFactory.jsonBuilder().startObject().startObject("MeanIndex").startObject("properties")
							.startObject("application").field("type", "string").endObject()
							.startObject("dateTimeJob").field("type", "date").endObject()
							.startObject("meanCpuUse").field("type", "double").endObject()
							.startObject("meanMemoryUse").field("type", "double").endObject()
							.startObject("totalNetworkTraffic").field("type", "long").endObject();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				GenMeanIndex.PutMapping(meanMapping);
			}
		} else {
			result = true;
		}

		return result;
	}

	public static boolean GenMaxMinCreatedIndexElasticSearch(String esClusterName, String esHost, Integer esPort, Integer esShards, Integer esNrep, String meanMapLabel, String maxMinMapLabel){

		Calendar calendario = GregorianCalendar.getInstance();
		Date fecha = calendario.getTime();
		SimpleDateFormat formatoDeFecha = new SimpleDateFormat("yyyy.MM.dd");

		boolean result = false; 

		ElasticSearchIndex GenMaxMinIndex = new ElasticSearchIndex(esClusterName, esHost, esPort, "storm-" + formatoDeFecha.format(fecha), maxMinMapLabel, esShards, esNrep);


		if(!GenMaxMinIndex.IndexExists()){
			result = GenMaxMinIndex.CreateIndex();

			if(result){
				XContentBuilder maxMinMapping = null;
				try {
					maxMinMapping = XContentFactory.jsonBuilder().startObject().startObject("MaxMinIndex").startObject("properties")
							.startObject("application").field("type", "string").endObject()
							.startObject("dateTimeJob").field("type", "date").endObject()
							.startObject("maxCpuUse").field("type", "integer").endObject()
							.startObject("minCpuUse").field("type", "integer").endObject()
							.startObject("maxMemoryUse").field("type", "integer").endObject()
							.startObject("minMemoryUse").field("type", "integer").endObject()
							.startObject("operatingSystem").field("type", "string").endObject()
							.startObject("city").field("type", "string").endObject()
							.startObject("country").field("type", "string").endObject()
							.startObject("typeEvent").field("type", "string").endObject()
							.startObject("cpuUse").field("type", "integer").endObject()
							.startObject("memoryUse").field("type", "integer").endObject()
							.startObject("networkStatus").field("type", "string").endObject()
							.startObject("systemError").field("type", "string").endObject()
							.startObject("dateTimeEvent").field("type", "date").endObject()
							.startObject("dateTimeProcessed").field("type", "date").endObject()
							.startObject("user").field("type", "string").endObject()
							.startObject("company").field("type", "string").endObject()
							.startObject("ipServer").field("type", "string").endObject()
							.startObject("ipHost").field("type", "string").endObject()
							.startObject("postalCode").field("type", "string").endObject()
							.startObject("state").field("type", "string").endObject()
							.startObject("networkTraffic").field("type", "long").endObject();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				GenMaxMinIndex.PutMapping(maxMinMapping);
			}
		} else {
			result = true;
		}

		return result;
	}

	public static boolean AppMeanCreatedIndexElasticSearch(String esClusterName, String esHost, Integer esPort, Integer esShards, Integer esNrep, String meanMapLabel, String maxMinMapLabel){

		Calendar calendario = GregorianCalendar.getInstance();
		Date fecha = calendario.getTime();
		SimpleDateFormat formatoDeFecha = new SimpleDateFormat("yyyy.MM.dd");

		boolean result = false; 

		ElasticSearchIndex AppMeanIndex = new ElasticSearchIndex(esClusterName, esHost, esPort, "storm-" + formatoDeFecha.format(fecha), meanMapLabel, esShards, esNrep);

		if(!AppMeanIndex.IndexExists()){
			result = AppMeanIndex.CreateIndex();

			if(result){
				XContentBuilder meanMapping = null;
				try {
					meanMapping = XContentFactory.jsonBuilder().startObject().startObject("MeanIndex").startObject("properties")
							.startObject("application").field("type", "string").endObject()
							.startObject("dateTimeJob").field("type", "date").endObject()
							.startObject("meanCpuUse").field("type", "double").endObject()
							.startObject("meanMemoryUse").field("type", "double").endObject()
							.startObject("totalNetworkTraffic").field("type", "long").endObject();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				AppMeanIndex.PutMapping(meanMapping);
			}
		} else {
			result = true;
		}

		return result;
	}

	public static boolean AppMaxMinCreatedIndexElasticSearch(String esClusterName, String esHost, Integer esPort, Integer esShards, Integer esNrep, String meanMapLabel, String maxMinMapLabel){

		Calendar calendario = GregorianCalendar.getInstance();
		Date fecha = calendario.getTime();
		SimpleDateFormat formatoDeFecha = new SimpleDateFormat("yyyy.MM.dd");

		boolean result = false; 

		ElasticSearchIndex AppMaxMinIndex = new ElasticSearchIndex(esClusterName, esHost, esPort, "storm-" + formatoDeFecha.format(fecha), maxMinMapLabel, esShards, esNrep);

		if(!AppMaxMinIndex.IndexExists()){
			result = AppMaxMinIndex.CreateIndex();

			if(result){
				XContentBuilder maxMinMapping = null;
				try {
					maxMinMapping = XContentFactory.jsonBuilder().startObject().startObject("MaxMinIndex").startObject("properties")
							.startObject("application").field("type", "string").endObject()
							.startObject("dateTimeJob").field("type", "date").endObject()
							.startObject("maxCpuUse").field("type", "integer").endObject()
							.startObject("minCpuUse").field("type", "integer").endObject()
							.startObject("maxMemoryUse").field("type", "integer").endObject()
							.startObject("minMemoryUse").field("type", "integer").endObject()
							.startObject("operatingSystem").field("type", "string").endObject()
							.startObject("city").field("type", "string").endObject()
							.startObject("country").field("type", "string").endObject()
							.startObject("typeEvent").field("type", "string").endObject()
							.startObject("cpuUse").field("type", "integer").endObject()
							.startObject("memoryUse").field("type", "integer").endObject()
							.startObject("networkStatus").field("type", "string").endObject()
							.startObject("systemError").field("type", "string").endObject()
							.startObject("dateTimeEvent").field("type", "date").endObject()
							.startObject("dateTimeProcessed").field("type", "date").endObject()
							.startObject("user").field("type", "string").endObject()
							.startObject("company").field("type", "string").endObject()
							.startObject("ipServer").field("type", "string").endObject()
							.startObject("ipHost").field("type", "string").endObject()
							.startObject("postalCode").field("type", "string").endObject()
							.startObject("state").field("type", "string").endObject()
							.startObject("networkTraffic").field("type", "long").endObject();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				AppMaxMinIndex.PutMapping(maxMinMapping);
			}
		} else {
			result = true;
		}

		return result;
	}


}
