package com.bridgetct.core;

import java.net.InetAddress;
import java.net.UnknownHostException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.LoggerFactory;

public class BridgetctSearchMoudel {
	
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BridgetctSearchMoudel.class);
	
	private static Client getEsClient() throws UnknownHostException {
		Settings settings = Settings.builder()
			.put("cluster.name", "elasticsearch")
			//x-pack 설치시
			//.put("xpack.security.user", "transport_client_user:changeme")
			.put("client.transport.ignore_cluster_name", true)
			.build();
		return new PreBuiltTransportClient(settings)
			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("100.100.107.82"), 9300));
		
	}
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		long endTime = 0;
		Client client = null;
		SearchResponse res = null;
		JSONArray jSONArray = new JSONArray();
		
		
		try {
			AggregationBuilder aggregation =
				    AggregationBuilders
				        .nested("nested", "word")
				        .subAggregation(
				                AggregationBuilders
				                        .terms("result").field("word.words")
				                        .subAggregation(
				                                AggregationBuilders
				                                .terms("result2").field("word.words")
				                        )
				        );
			client = getEsClient();
			res = client.prepareSearch("join_data")
		        .setTypes("content")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
		        .addAggregation(aggregation)
		        .get();
			Nested nes = res.getAggregations().get("nested");
//			Terms agg = nes.getAggregations().get("word_result");
			Terms agg = nes.getAggregations().get("result");
			for (Terms.Bucket entry : agg.getBuckets()){
				Terms agg1 = entry.getAggregations().get("result2");
				
				JSONObject jSONObject = new JSONObject();
				jSONObject.put("key", entry.getKey());
				jSONObject.put("value", entry.getDocCount());
				
				JSONArray jSONArray2 = new JSONArray();
				for (Terms.Bucket entry2 : agg.getBuckets()) {
					JSONObject jSONObject2 = new JSONObject();
					jSONObject2.put("key", entry2.getKey());
					jSONObject2.put("value", entry2.getDocCount());
					jSONArray2.add(jSONObject2);
				}
				jSONObject.put("array", jSONArray2);
				jSONArray.add(jSONObject);
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		endTime = System.currentTimeMillis();
		System.out.println("##  소요시간(초.0f) : " + ( endTime - startTime )/1000.0f +"초"); 
        System.out.println("=========================== 완료 ========================");
		System.out.println(jSONArray);
	}
	
	/**
	 * com.bridegetec.core
	 * BridegetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: 단일 데이터 조회 (한건의 데이터만 조회한다.)
	 */
	public JSONObject getMustDataTable(){
		Client client = null;
		SearchResponse res = null;
		JSONObject jSONObject = new JSONObject();
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("_id", "AVy-t3H2vofwsvFFjo2a"))
		        .get();
		
			for (SearchHit hit : res.getHits().getHits()) {
				jSONObject.putAll(hit.getSource());
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		return jSONObject;
	}
	
	/**
	 * com.bridegetec.core
	 * BridegetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: 다중조건 Array 출력
	 */
	public JSONArray getMustDataTables() {
		Client client = null;
		SearchResponse res = null;
		JSONArray jSONArray = new JSONArray();
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("ucid_gkey", "20170502090354_2465_1902_5907ccea_7.VOC"))
		        .setQuery(QueryBuilders.termQuery("armsoffset", "78860"))
		        .get();
			for (SearchHit hit : res.getHits().getHits()) {
				jSONArray.add(hit.getSource());
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		
		return jSONArray;
	}
	
	/**
	 * com.bridegetec.core
	 * BridegetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: MustNot 해당 값과 불일치 하는 데이터를 찾는다.
	 */
	public JSONArray getMustNotDataTables() {
		Client client = null;
		SearchResponse res = null;
		JSONArray jSONArray = new JSONArray();
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("ucid_gkey", "20170502090354_2465_1902_5907ccea_7.VOC")))
		        .get();
			for (SearchHit hit : res.getHits().getHits()) {
				jSONArray.add(hit.getSource());
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		
		return jSONArray;
	}
	
	/**
	 * com.bridegetec.core
	 * BridegetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: Should는 RDBMS 조건절에서 or 연산자와 동일
	 */
	public JSONArray getShouldDataTables() {
		Client client = null;
		SearchResponse res = null;
		JSONArray jSONArray = new JSONArray();
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("ucid_gkey", "20170502090354_2465_1902_5907ccea_7.VOC")))
		        .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("ucid_gkey", "20170502090829_2466_1902_5907cdfd_4.VOC")))
		        .get();
			for (SearchHit hit : res.getHits().getHits()) {
				jSONArray.add(hit.getSource());
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		
		return jSONArray;
	}
	
	/**
	 * com.bridegetec.core
	 * BridegetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: 단일 통계 처리
	 */
	public static JSONArray getAggregationDataTable() {
		Client client = null;
		SearchResponse res = null;
		JSONArray jSONArray = new JSONArray();
		
		
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.matchAllQuery())
		        .addAggregation(AggregationBuilders.terms("word_result").field("word"))
		        .get();
			Terms agg = res.getAggregations().get("word_result");
			for (Terms.Bucket entry : agg.getBuckets()){
				JSONObject jSONObject = new JSONObject();
				jSONObject.put("key", entry.getKey());
				jSONObject.put("value", entry.getDocCount());
				jSONArray.add(jSONObject);
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		
		return jSONArray;
	}
	
	/**
	 * com.bridegetec.core
	 * BridgetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: 다중 통계 처리
	 */
	public static JSONArray getAggregationDataTables() {
		Client client = null;
		SearchResponse res = null;
		JSONArray jSONArray = new JSONArray();
		
		
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.matchAllQuery())
		        .addAggregation(AggregationBuilders.terms("word_result").field("word"))
		        .addAggregation(AggregationBuilders.terms("word_result2").field("armsoffset"))
		        .get();
			Terms agg = res.getAggregations().get("word_result");
			for (Terms.Bucket entry : agg.getBuckets()){
				Terms agg1 = entry.getAggregations().get("word_result2");
				
				JSONObject jSONObject = new JSONObject();
				jSONObject.put("key", entry.getKey());
				jSONObject.put("value", entry.getDocCount());
				
				JSONArray jSONArray2 = new JSONArray();
				for (Terms.Bucket entry2 : agg.getBuckets()) {
					JSONObject jSONObject2 = new JSONObject();
					jSONObject2.put("key", entry2.getKey());
					jSONObject2.put("value", entry2.getDocCount());
					jSONArray2.add(jSONObject2);
				}
				jSONObject.put("array", jSONArray2);
				jSONArray.add(jSONObject);
			}
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		
		return jSONArray;
	}
	
	/**
	 * com.bridegetec.core
	 * BridgetctSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 20.
	 * @설명	: 검색된 데이터 count
	 */
	public long getDataCount() {
		Client client = null;
		SearchResponse res = null;
		long count = 0;
		try {
			client = getEsClient();
			res = client.prepareSearch("array_data")
		        .setTypes("row")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("ucid_gkey", "20170502090354_2465_1902_5907ccea_7.VOC")))
		        .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("ucid_gkey", "20170502090829_2466_1902_5907cdfd_4.VOC")))
		        .get();
			count = res.getHits().getTotalHits();
		} catch (UnknownHostException e) {
			logger.error("error" + logger);
		} finally {
			client.close();
		}
		
		return count;
	}
	
}
