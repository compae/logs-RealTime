package project.ebd;

public class ConfigRes {
	
	public static final String REDIS_HOST = "localhost";
	public static final String REDIS_PORT = "6379";
	
	public static final String REDIS_TOTAL_FIELD = "totalWords";
	public static final String REDIS_WORDS_FIELD = "words";
	public static final String REDIS_BIGRAMS_FIELD = "bigrams";
	public static final String REDIS_TRIGRAMS_FIELD = "trigrams";
	public static final String REDIS_LINES_FIELD = "totalLines";
	
	public static final String RABBITMQ_HOST = "localhost";
	public static final String RABBITMQ_PORT = "5672";
	public static final String RABBITMQ_USERNAME = "guest";
	public static final String RABBITMQ_PASSWORD = "guest";
	public final static String RABBITMQ_QUEUE_LOGS = "stormQueue";
	public final static String RABBITMQ_EXCHANGE_LOGS = "stormExchange";
	public final static String RABBITMQ_ROUTING_KEY_LOGS = "stormRoute";
	
	public final static String RABBITMQ_QUEUE_JOBS = "jobsQueue";
	public final static String RABBITMQ_EXCHANGE_JOBS = "jobsExchange";
	public final static String RABBITMQ_ROUTING_KEY_JOBS = "jobsRoute";
	
	public final static String GEN_KEY = "GENERAL";
	
	public final static Integer MAX_SYSTEM_ERRORS = 5000;
	public final static Integer MAX_NETWORK_ERRORS = 100;
	
	public final static Integer MAX_APP_SYSTEM_ERRORS = 2500;
	public final static Integer MAX_APP_NETWORK_ERRORS = 25;
	
	public final static String ES_CLUSTER_NAME = "elasticsearch-logs";
	public final static String ES_HOST = "127.0.0.1";
	public final static Integer ES_PORT = 9309;
	public final static Integer ES_SHARDS = 5;
	public final static Integer ES_NREP = 1;
	
	public final static String GEN_INDEX_LABEL = "genkey";
	public final static String APP_INDEX_LABEL = "appkey";
	public final static String MEAN_MAPPING_LABEL = "MeanIndex";
	public final static String MAXMIN_MAPPING_LABEL = "MaxMinIndex";

}
