package project.ebd;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
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
			
			conf.put("rabbitUserName", ConfigRes.RABBITMQ_USERNAME);
			conf.put("rabbitPassword", ConfigRes.RABBITMQ_PASSWORD);
			conf.put("rabbitHost", ConfigRes.RABBITMQ_HOST);
			conf.put("rabbitPort", ConfigRes.RABBITMQ_PORT);
			
			conf.put("redisHost", ConfigRes.REDIS_HOST);
			conf.put("redisPort", ConfigRes.REDIS_PORT);
			
			conf.put("stormExchange", ConfigRes.RABBITMQ_EXCHANGE_LOGS);
			conf.put("stormQueue", ConfigRes.RABBITMQ_QUEUE_LOGS);
			conf.put("stormRoute", ConfigRes.RABBITMQ_ROUTING_KEY_LOGS);
			
			conf.put("jobsExchange", ConfigRes.RABBITMQ_EXCHANGE_JOBS);
			conf.put("jobsQueue", ConfigRes.RABBITMQ_QUEUE_JOBS);
			conf.put("jobsRoute", ConfigRes.RABBITMQ_ROUTING_KEY_JOBS);
			
			conf.put("sysErrNum", ConfigRes.MAX_SYSTEM_ERRORS);
			conf.put("netErrNum", ConfigRes.MAX_NETWORK_ERRORS);
			conf.put("sysAppErrNum", ConfigRes.MAX_APP_SYSTEM_ERRORS);
			conf.put("netAppErrNum", ConfigRes.MAX_APP_NETWORK_ERRORS);
			
			conf.put("esClusterName", ConfigRes.ES_CLUSTER_NAME);
			conf.put("esHost", ConfigRes.ES_HOST);
			conf.put("esPort", ConfigRes.ES_PORT);
			conf.put("esShards", ConfigRes.ES_SHARDS);
			conf.put("esNrep", ConfigRes.ES_NREP);
			conf.put("meanMapLabel", ConfigRes.MEAN_MAPPING_LABEL);
			conf.put("maxMinMapLabel", ConfigRes.MAXMIN_MAPPING_LABEL);
			conf.put("genLabel", ConfigRes.GEN_INDEX_LABEL);
			conf.put("appLabel", ConfigRes.APP_INDEX_LABEL);
			
			conf.put("genKey", ConfigRes.GEN_KEY);
			
			GenMeanIdxCreated = IndexesMappings.GenMeanCreatedIndexElasticSearch(ConfigRes.ES_CLUSTER_NAME, ConfigRes.ES_HOST, ConfigRes.ES_PORT, ConfigRes.ES_SHARDS, ConfigRes.ES_NREP,
					ConfigRes.MEAN_MAPPING_LABEL, ConfigRes.MAXMIN_MAPPING_LABEL);
			GenMaxMinIdxCreated = IndexesMappings.GenMaxMinCreatedIndexElasticSearch(ConfigRes.ES_CLUSTER_NAME, ConfigRes.ES_HOST, ConfigRes.ES_PORT, ConfigRes.ES_SHARDS, ConfigRes.ES_NREP,
					ConfigRes.MEAN_MAPPING_LABEL, ConfigRes.MAXMIN_MAPPING_LABEL);
			AppMeanIdxCreated = IndexesMappings.AppMeanCreatedIndexElasticSearch(ConfigRes.ES_CLUSTER_NAME, ConfigRes.ES_HOST, ConfigRes.ES_PORT, ConfigRes.ES_SHARDS, ConfigRes.ES_NREP,
					ConfigRes.MEAN_MAPPING_LABEL, ConfigRes.MAXMIN_MAPPING_LABEL);
			AppMaxMinIdxCreated = IndexesMappings.AppMaxMinCreatedIndexElasticSearch(ConfigRes.ES_CLUSTER_NAME, ConfigRes.ES_HOST, ConfigRes.ES_PORT, ConfigRes.ES_SHARDS, ConfigRes.ES_NREP,
					ConfigRes.MEAN_MAPPING_LABEL, ConfigRes.MAXMIN_MAPPING_LABEL);
			
			conf.put("GenMeanIdxCreated", GenMeanIdxCreated);
			conf.put("GenMaxMinIdxCreated", GenMaxMinIdxCreated);
			conf.put("AppMeanIdxCreated", AppMeanIdxCreated);
			conf.put("AppMaxMinIdxCreated", AppMaxMinIdxCreated);
			
			conf.setDebug(false);
			
			builderLogs.setSpout("ReaderLogsSplout", new LogsSpout(), 2);
			builderLogs.setBolt("GeneralBolt", new GeneralBolt(), 5).shuffleGrouping("ReaderLogsSplout");
			builderLogs.setBolt("MetricsGeneralBolt", new MetricsGeneralBolt(), 5).shuffleGrouping("GeneralBolt");
			builderLogs.setBolt("AlertBolt", new AlertBolt(), 2).shuffleGrouping("MetricsGeneralBolt");
			
			if((args.length == 3) && (args[1].equals("-application"))){
				conf.put("applications", args[2]);
				builderLogs.setBolt("AppBolt", new ApplicationBolt(),5).shuffleGrouping("ReaderLogsSplout");
				builderLogs.setBolt("MetricsAppBolt", new MetricsApplicationBolt(), 5).shuffleGrouping("AppBolt");
				builderLogs.setBolt("AlertAppBolt", new AlertBolt(), 2).shuffleGrouping("MetricsAppBolt");
			}
			
			builderLogs.setSpout("ReaderJobsSplout", new JobsSpout(),1);
			builderLogs.setBolt("JobsBolt", new JobsBolt(),2).shuffleGrouping("ReaderJobsSplout");
			builderLogs.setBolt("JobsExecutionBolt", new JobsExecutionBolt(),5).shuffleGrouping("JobsBolt");
			
			builderLogs.setBolt("GenMeanIndexBolt", new GenMeanIndexBolt(),10).shuffleGrouping("JobsExecutionBolt", "streamMeanGenLabel");
			builderLogs.setBolt("GenMaxMinIndexBolt", new GenMaxMinIndexBolt(),10).shuffleGrouping("JobsExecutionBolt", "streamMaxMinGenLabel");
			builderLogs.setBolt("AppMeanIndexBolt", new AppMeanIndexBolt(),10).shuffleGrouping("JobsExecutionBolt", "streamMeanAppLabel");
			builderLogs.setBolt("AppMaxMinIndexBolt", new AppMaxMinIndexBolt(),10).shuffleGrouping("JobsExecutionBolt", "streamMaxMinAppLabel");	
	
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