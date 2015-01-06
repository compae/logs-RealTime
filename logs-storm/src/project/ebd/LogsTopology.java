package project.ebd;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class LogsTopology {	
	
	static boolean GenMeanIdxCreated = false;
	static boolean GenMaxMinIdxCreated = false;
	static boolean AppMeanIdxCreated = false;
	static boolean AppMaxMinIdxCreated = false;
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException{

		if((args.length == 1) || (args.length == 3))  {
			
			TopologyBuilder builderLogs = new TopologyBuilder();
			
			Config conf = new Config();
			
			conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 16);
			conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
			conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
			conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
			
			conf.put("rabbitUserName", Configuration.RABBITMQ_USERNAME);
			conf.put("rabbitPassword", Configuration.RABBITMQ_PASSWORD);
			conf.put("rabbitHost", Configuration.RABBITMQ_HOST);
			conf.put("rabbitPort", Configuration.RABBITMQ_PORT);
			
			conf.put("redisHost", Configuration.REDIS_HOST);
			conf.put("redisPort", Configuration.REDIS_PORT);
			
			conf.put("stormExchange", Configuration.RABBITMQ_EXCHANGE_LOGS);
			conf.put("stormQueue", Configuration.RABBITMQ_QUEUE_LOGS);
			conf.put("stormRoute", Configuration.RABBITMQ_ROUTING_KEY_LOGS);
			
			conf.put("jobsExchange", Configuration.RABBITMQ_EXCHANGE_JOBS);
			conf.put("jobsQueue", Configuration.RABBITMQ_QUEUE_JOBS);
			conf.put("jobsRoute", Configuration.RABBITMQ_ROUTING_KEY_JOBS);
			
			conf.put("sysErrNum", Configuration.MAX_SYSTEM_ERRORS);
			conf.put("netErrNum", Configuration.MAX_NETWORK_ERRORS);
			conf.put("sysAppErrNum", Configuration.MAX_APP_SYSTEM_ERRORS);
			conf.put("netAppErrNum", Configuration.MAX_APP_NETWORK_ERRORS);
			
			conf.put("esClusterName", Configuration.ES_CLUSTER_NAME);
			conf.put("esHost", Configuration.ES_HOST);
			conf.put("esPort", Configuration.ES_PORT);
			conf.put("esShards", Configuration.ES_SHARDS);
			conf.put("esNrep", Configuration.ES_NREP);
			conf.put("meanMapLabel", Configuration.MEAN_MAPPING_LABEL);
			conf.put("maxMinMapLabel", Configuration.MAXMIN_MAPPING_LABEL);
			conf.put("genLabel", Configuration.GEN_INDEX_LABEL);
			conf.put("appLabel", Configuration.APP_INDEX_LABEL);
			
			conf.put("genKey", Configuration.GEN_KEY);
			
			GenMeanIdxCreated = project.ebd.Utils.GenMeanCreatedIndexElasticSearch(Configuration.ES_CLUSTER_NAME, Configuration.ES_HOST, Configuration.ES_PORT, Configuration.ES_SHARDS, Configuration.ES_NREP,
					Configuration.MEAN_MAPPING_LABEL, Configuration.MAXMIN_MAPPING_LABEL);
			GenMaxMinIdxCreated = project.ebd.Utils.GenMaxMinCreatedIndexElasticSearch(Configuration.ES_CLUSTER_NAME, Configuration.ES_HOST, Configuration.ES_PORT, Configuration.ES_SHARDS, Configuration.ES_NREP,
					Configuration.MEAN_MAPPING_LABEL, Configuration.MAXMIN_MAPPING_LABEL);
			AppMeanIdxCreated = project.ebd.Utils.AppMeanCreatedIndexElasticSearch(Configuration.ES_CLUSTER_NAME, Configuration.ES_HOST, Configuration.ES_PORT, Configuration.ES_SHARDS, Configuration.ES_NREP,
					Configuration.MEAN_MAPPING_LABEL, Configuration.MAXMIN_MAPPING_LABEL);
			AppMaxMinIdxCreated = project.ebd.Utils.AppMaxMinCreatedIndexElasticSearch(Configuration.ES_CLUSTER_NAME, Configuration.ES_HOST, Configuration.ES_PORT, Configuration.ES_SHARDS, Configuration.ES_NREP,
					Configuration.MEAN_MAPPING_LABEL, Configuration.MAXMIN_MAPPING_LABEL);
			
			conf.put("GenMeanIdxCreated", GenMeanIdxCreated);
			conf.put("GenMaxMinIdxCreated", GenMaxMinIdxCreated);
			conf.put("AppMeanIdxCreated", AppMeanIdxCreated);
			conf.put("AppMaxMinIdxCreated", AppMaxMinIdxCreated);
			
			conf.setDebug(false);
			
			builderLogs.setSpout("ReaderLogsSplout", new LogsSpout(), 2);
			builderLogs.setBolt("GeneralBolt", new GeneralBolt(), 5).shuffleGrouping("ReaderLogsSplout");
			builderLogs.setBolt("MetricsGeneralBolt", new MetricsGeneralBolt(), 10).shuffleGrouping("GeneralBolt");
			builderLogs.setBolt("AlertBolt", new AlertBolt(), 2).shuffleGrouping("MetricsGeneralBolt");
			
			if((args.length == 3) && (args[1].equals("-application"))){
				conf.put("applications", args[2]);
				builderLogs.setBolt("AppBolt", new ApplicationBolt(),5).shuffleGrouping("ReaderLogsSplout");
				builderLogs.setBolt("MetricsAppBolt", new MetricsApplicationBolt(), 10).shuffleGrouping("AppBolt");
				builderLogs.setBolt("AlertAppBolt", new AlertBolt(), 2).shuffleGrouping("MetricsAppBolt");
			}
			
			builderLogs.setSpout("ReaderJobsSplout", new JobsSpout(),1);
			builderLogs.setBolt("JobsBolt", new JobsBolt(),5).shuffleGrouping("ReaderJobsSplout");
			builderLogs.setBolt("JobsExecutionBolt", new JobsExecutionBolt(),10).shuffleGrouping("JobsBolt");
			
			builderLogs.setBolt("GenMeanIndexBolt", new GenMeanIndexBolt(),20).fieldsGrouping("JobsExecutionBolt", new Fields("meanGenLabel"));//new Fields("meanGenLabel", "maxMinGenLabel","meanAppLabel", "maxMinAppLabel"));
			builderLogs.setBolt("GenMaxMinIndexBolt", new GenMaxMinIndexBolt(),20).fieldsGrouping("JobsExecutionBolt", new Fields("maxMinGenLabel"));
			builderLogs.setBolt("AppMeanIndexBolt", new AppMeanIndexBolt(),20).fieldsGrouping("JobsExecutionBolt", new Fields("meanAppLabel"));
			builderLogs.setBolt("AppMaxMinIndexBolt", new AppMaxMinIndexBolt(),10).fieldsGrouping("JobsExecutionBolt", new Fields("maxMinAppLabel"));

			
			if(args[0].equals("-cluster")){
				conf.setNumWorkers(2);
				StormSubmitter.submitTopology("LogsTopology", conf, builderLogs.createTopology());
			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("LogsTopology", conf, builderLogs.createTopology());
				Utils.sleep(4500000);

				cluster.killTopology("LogsTopology");
				cluster.shutdown();
			}
		} else {
			System.out.print("Incorrect arguments.");
		}
	}
}