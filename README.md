***** VERSIONES *****

RABBITMQ 3.4.2
ELASTICSEARCH 1.4.0
LOGSTASH 1.4.2
ZOOKEEPER 3.4.6
STORM 0.9.3
REDIS 2.8.19
KIBANA 3.1.2

***** INICIAR RABBITMQ *****

sudo service rabbitmq-server start

Acceso WEB UI:
	http://localhost:15672/
		user:guest
		pass:guest

***** INICIAR ELASTICSEARCH ******

vim ~/elasticsearch/elasticsearch-1.4.0/config/elasticsearch.yml

	cluster.name: elasticsearch-logs
	node.name: "ES-logstash"
	bootstrap.mlockall: true
	network.host: 127.0.0.1
	transport.tcp.port: 9309

~/elasticsearch/elasticsearch-1.4.0/bin/elasticsearch
~/elasticsearch/elasticsearch-1.4.0/bin/plugin -install lmenezes/elasticsearch-kopf

curl 'localhost:9200/logstash-2014.11.11/_search?q=*&pretty'

Acceso WEB UI:
	http://127.0.0.1:9201/_plugin/head/
	http://127.0.0.1:9201/_plugin/bigdesk/#nodes
	http://127.0.0.1:9201/_plugin/kopf



***** INICIAR LOGSTASH *****

vim ~/logstash/logstash-1.4.2/bin/logstash-simple.conf

	input { 
	  rabbitmq {
	    	host => "localhost"
	    	queue => "logsQueue"
	    	exchange => "logsExchange"
		key => "logsRoute"
		durable => true 
	  } 
	}
	output {
	  elasticsearch { 
		host => "127.0.0.1"
		port => "9309"
	  	cluster => "elasticsearch-logs"
	  	node_name => "ES-logstash"
	  }
	  rabbitmq {
	   	exchange => "stormExchange"
	  	host => "localhost"
	  	exchange_type => "direct"
	    	key => "stormRoute"
	    	durable => true 
	  }
	}

~/logstash/logstash-1.4.2/bin/logstash agent -f ~/logstash/logstash-1.4.2/bin/logstash-simple.conf

ps aux | grep logstash
ps aux | grep elasticsearch


***** INICIAR ZOOKEEPER *****

vim ~/zookeeper/zookeeper-3.4.6/conf/zoo.cfg

	tickTime=2000
	initLimit=10
	syncLimit=5
	dataDir=/tmp/zookeeper
	clientPort=2181

sudo ~/zookeeper/zookeeper-3.4.6/bin/zkServer.sh start


***** INICIAR STORM *****

vim ~/storm/apache-storm-0.9.3/conf/storm.yaml

	storm.zookeeper.servers:
     - "localhost"
    nimbus.host: "localhost"

~/storm/apache-storm-0.9.3/bin/storm supervisor
~/storm/apache-storm-0.9.3/bin/storm nimbus
~/storm/apache-storm-0.9.3/bin/storm ui

Acceso WEB UI:
	http://127.0.0.1:8080/index.html


***** JAR STORM TOPOLOGY *****

storm jar logs-storm-0.0.1-SNAPSHOT-jar-with-dependencies.jar main.java.storm.ebd.LogsTopology -cluster -application 'WEB SERVER'

			conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 16);
			conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
			conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
			conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);


***** JAR GENERADOR DE EVENTOS BENERATOR *****

MAVEN:
${workspace_loc:/data-generator-master}
clean install benerator:generate

java jar ~/workspace/logs-generator/target/logs-generator-1.0-SNAPSHOT.jar


***** JAR GENERADOR DE TRABAJOS *****

java jar ~/workspace/jobs-generator/target/jobs-generator-1.0-SNAPSHOT.jar -debug -application 'WEB_SERVER,LOGGIN_SERVER'


***** REDIS QUERIES *****

flushdb

get totalLogs
get totalJobs

hget "GENERAL" count
hget "GENERAL" cpuUse
hget "GENERAL" memoryUse
hget "GENERAL" networkTraffic

hget "GENERAL" maxCpuUse
hget "GENERAL" minCpuUse
hget "GENERAL" minCpuUseData
hget "GENERAL" maxCpuUseData
hget "GENERAL" maxMemoryUse
hget "GENERAL" minMemoryUse
hget "GENERAL" maxMemoryUseData
hget "GENERAL" minMemoryUseData

hget "GENERAL" systemError
hget "GENERAL" networkStatus
hget "GENERAL" "systemError WINDOWS_8"
hget "GENERAL" "networkStatus PAYNEVILLE"

Probar con: "WEB_SERVER"  "LOGGIN_SERVER"