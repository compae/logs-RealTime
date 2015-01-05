package project.ebd;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class ElasticSearchIndex {
	
	private Settings settings;
	private Client client;
	private String indexName;
	private String indexType;
	private Integer numShards;
	private Integer numRep; 
	
	public ElasticSearchIndex(String clusterName, String hostName, Integer port){
		
		if(!clusterName.equals("")){
			settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).put("index.number_of_shards", numShards).put("index.number_of_replicas", numRep).build();
			if(!hostName.equals("") && (port != 0)){
				client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostName, port));
			}
		}
		
	}
	
	public ElasticSearchIndex(){
		
		settings = null;
		client = null;
		indexName = "";
		indexType = "";
		numShards = 1;
		numRep = 1;
		
	}

	
	public ElasticSearchIndex(String clusterName, String hostName, Integer port, String index, String type, Integer nShards, Integer nRep){
		
		if(!clusterName.equals("")){
			settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).put("index.number_of_shards", nShards).put("index.number_of_replicas", nRep).build();
			//settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
			if(!hostName.equals("") && (port != 0)){
				client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostName, port));
			}
		}
	
		indexName = index;
		indexType = type;
		numShards = nShards;
		numRep = nRep;

	}

	public void SetAllSettings(String clusterName, String hostName, Integer port, Integer nShards, Integer nRep){
		
		if(!clusterName.equals("")){
			settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).put("index.number_of_shards", nShards).put("index.number_of_replicas", nRep).build();
			//settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
			if(!hostName.equals("") && (port != 0)){
				client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostName, port));
			}
		}

		numShards = nShards;
		numRep = nRep;
		
	}
	
	public void SetConfiguration(String clusterName, String hostName, Integer port){
		
		if(!clusterName.equals("")){
			settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).put("index.number_of_shards", numShards).put("index.number_of_replicas", numRep).build();
			if(!hostName.equals("") && (port != 0)){
				client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(hostName, port));
			}
		}
	}
	
	public void SetIndexNameType(String index, String type){
		
		indexName = index;
		indexType = type;
	}
	
	public void SetShardRep(Integer nShards, Integer nRep){
		
		numShards = nShards;
		numRep = nRep;
	}
	
	public void SetIndexName(String index){
		
		indexName = index;
	}
	
	public void SetIndexType(String type){
		indexType = type;
	}
	
	public boolean IndexExists(){
		
		if(!indexName.equals("")){
			IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(indexName).execute().actionGet();	//.exists(new IndicesExistsRequest("indexName"));
			
			return existsResponse.isExists();
		} else {
			
			return false;
		}
	}
	
	public String GetStringDocument(String id){
		
		if(!indexName.equals("") && !indexType.equals("") && (client != null)){
			GetResponse response = client.prepareGet(indexName, indexType, id).execute().actionGet();
			
			return response.getSourceAsString();
		} else {
			return "";
		}
	}
	
	public boolean PutMapping(XContentBuilder aceMapping){

		if(!indexName.equals("") && !indexType.equals("") && (client != null)){
			PutMappingResponse mappingResponse = client.admin().indices().preparePutMapping(indexName).setType(indexType).setSource(aceMapping).execute().actionGet();
			
			return mappingResponse.isAcknowledged();
		} else {
			return false;
		}
	}
	
	public String DeleteDocsFromField(String field, String value){
		
		if(!indexName.equals("") && !indexType.equals("") && (client != null)){
			DeleteByQueryResponse delResponse = client.prepareDeleteByQuery(indexName)
				.setTypes("_type", indexType)
				.setQuery(QueryBuilders.termQuery(field, value))
		        .execute()
		        .actionGet();
			
			return delResponse.status().name();
		} else {
			return "";
		}
	}
		
	public boolean DeleteIndex(){
		
		if(!indexName.equals("") && (client != null)){
			DeleteIndexRequest request = new DeleteIndexRequest(indexName);
			DeleteIndexResponse response = client.admin().indices().delete(request).actionGet();
			
			return response.isAcknowledged();
		} else {
			return false;
		}
	}
	
	public boolean CreateAlias(String alias){
		
		if(!indexName.equals("") && !alias.equals("") && (client != null)){
			IndicesAliasesResponse aliasReponse = client.admin().indices().prepareAliases()
				.addAlias(indexName, alias).execute().actionGet();
			
			return aliasReponse.isAcknowledged();
		} else {
			return false;
		}
	}
	
	public boolean InsertDocument(String id, XContentBuilder builder){
		
		if(!indexName.equals("") && !indexType.equals("") && (client != null)){
			IndexResponse insertResponse;
			if(!id.equals("")){
				insertResponse = client.prepareIndex(indexName, indexType, id).setSource(builder).execute().actionGet();
			} else {
				insertResponse = client.prepareIndex(indexName, indexType).setSource(builder).execute().actionGet();
			}
			
			return insertResponse.isCreated();
		} else {
			return false;
		}
	}
	
	public long CountIndex(String field, String value){
		
		if(!indexName.equals("") && !indexType.equals("") && (client != null)){
			CountResponse countResponse = client.prepareCount(indexName)
		        .setQuery(QueryBuilders.termQuery("_type", indexType))
		        .setQuery(QueryBuilders.termQuery(field, value))
		        .execute()
		        .actionGet();
			
			return countResponse.getCount();
		} else {
			return 0;
		}
	}
	
	public void Close(){
		
		client.close();
	}
	
	public String GetClusterState(){
		
		if(client != null){
			ClusterStateRequestBuilder request=client.admin().cluster().prepareState();
			ClusterStateResponse responseCluster=request.execute().actionGet();
			
			return responseCluster.getState().toString();
		} else {
			return "";
		}
	}
	
	public boolean CreateIndex(){
		
		if(!indexName.equals("") && !indexType.equals("") && (client != null) && (settings != null)){
			//CreateIndexResponse createIndexResponse = client.admin().indices().prepareCreate(indexName).execute().actionGet();
			CreateIndexResponse createIndexResponse = client.admin().indices().create(Requests.createIndexRequest(indexName).settings(settings)).actionGet();
			return createIndexResponse.isAcknowledged();
		} else {
			return false;
		}
	}
		 
}
