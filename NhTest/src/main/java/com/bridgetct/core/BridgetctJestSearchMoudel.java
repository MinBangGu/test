package com.bridgetct.core;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.DateHistogramAggregation;
import io.searchbox.core.search.aggregation.DateHistogramAggregation.DateHistogram;
import io.searchbox.core.search.aggregation.TermsAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation.Entry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class BridgetctJestSearchMoudel {
	
	private static SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
	
	public static JestClient getJestClient() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig
				.Builder("http://100.100.107.82:9200")
				.multiThreaded(true)
				.build());
		
		return factory.getObject();
	}
	
	
	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		long endTime = 0;

		System.out.println(getMultipleData());
		endTime = System.currentTimeMillis();
		System.out.println("##  소요시간(초.0f) : " + ( endTime - startTime )/1000.0f +"초"); 
        System.out.println("=========================== 완료 ========================");
        
	}
	
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 21.
	 * @설명	: 단일 데이터 조회 (한건의 데이터만 조회한다.)
	 */
	public static Map getSingleData(){
		JestClient jestClient = null;
		Search search = null;
		JestResult result = null;
		Map data = new HashMap();
		
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.termQuery("_id", "AVzh8U8x7VJ8fZKdJdY3"));
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			
			result = jestClient.execute(search);
			
			data = result.getSourceAsObject(Map.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 21.
	 * @설명	: 다중조건 Array 출력
	 */
	public static List<Map> getMultipleData() {
		JestClient jestClient = null;
		Search search = null;
		JestResult result = null;
		List<Map> data = new ArrayList<Map>();
		
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("ucid_gkey", "20170609131428_2463_1902_593a20a4_6.VOC")).filter(QueryBuilders.termQuery("armsoffset", "567270"))
					);
			System.out.println(searchSourceBuilder.toString());
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			
			result = jestClient.execute(search);
			
			data = result.getSourceAsObjectList(Map.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 21.
	 * @설명	: MustNot 해당 값과 불일치 하는 데이터를 찾는다.
	 */
	public static List<Map> getMustNotData() {
		JestClient jestClient = null;
		Search search = null;
		JestResult result = null;
		List<Map> data = new ArrayList<Map>();
		
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.boolQuery()
					.mustNot(QueryBuilders.termQuery("ucid_gkey", "20170609131428_2463_1902_593a20a4_6.VOC")));
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			
			result = jestClient.execute(search);
			
			data = result.getSourceAsObjectList(Map.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 21.
	 * @설명	: Should는 RDBMS 조건절에서 or 연산자와 동일
	 */
	public static List<Map> getShouldData() {
		JestClient jestClient = null;
		Search search = null;
		JestResult result = null;
		List<Map> data = new ArrayList<Map>();
		
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.boolQuery()
					.should(QueryBuilders.termQuery("ucid_gkey", "20170609131428_2463_1902_593a20a4_6.VOC"))
					.should(QueryBuilders.termQuery("ucid_gkey", "20170502090829_2466_1902_5907cdfd_4.VOC")));
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			
			result = jestClient.execute(search);
			
			data = result.getSourceAsObjectList(Map.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 21.
	 * @설명	: 단일 통계 처리
	 */
	public static List<Map> getAggreagtionData() {
		JestClient jestClient = null;
		Search search = null;
		SearchResult result = null;
		List<Map> data = new ArrayList<Map>();
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.matchAllQuery());
			searchSourceBuilder.aggregation(AggregationBuilders.terms("word_result").field("word").shardSize(10));

			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			result = jestClient.execute(search);
			
			TermsAggregation terms = result.getAggregations().getTermsAggregation("word_result");
			
			for (Entry entry :  terms.getBuckets()) {
				Map bucket = new HashMap();
				bucket.put("key", entry.getKey());
				bucket.put("value", entry.getCount());
				data.add(bucket);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
		
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 6. 21.
	 * @설명	: 다중 통계 처리 ( nested 필드로 변형되면서 사용 X )
	 */
	public static List<Map> getMultiAggreagtionData() {
		JestClient jestClient = null;
		Search search = null;
		SearchResult result = null;
		List<Map> data = new ArrayList<Map>();
		try {
			jestClient = getJestClient();
			AggregationBuilder aggregation =
				    AggregationBuilders
				    	.terms("word_result").field("word").shardSize(10)
				        .subAggregation(
				                AggregationBuilders
				                        .terms("word_result_bottom").field("word").shardSize(10)
				        );
			searchSourceBuilder.query(QueryBuilders.matchAllQuery());
			searchSourceBuilder.aggregation(aggregation);
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("array_data")
					.addType("row").build();
			result = jestClient.execute(search);
			
			TermsAggregation terms = result.getAggregations().getTermsAggregation("word_result");
			for (Entry entry :  terms.getBuckets()) {
				Map bucket = new HashMap();
				bucket.put("key", entry.getKey());
				bucket.put("value", entry.getCount());
				
				List<Map> data_bottom = new ArrayList<Map>();
				TermsAggregation terms_bottom = entry.getAggregation("word_result_bottom",TermsAggregation.class);
				for (Entry entry_bottom :  terms.getBuckets()) {
					Map bucket_bottom = new HashMap();
					bucket_bottom.put("key", entry_bottom.getKey());
					bucket_bottom.put("value", entry_bottom.getCount());
					data_bottom.add(bucket_bottom);
				}
				bucket.put("bottom", data_bottom);
				data.add(bucket);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
		
	}
	
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 7. 17.
	 * @설명	: parent - child type 구조의 데이터 출력 ( 녹취검색 )
	 */
	public static List<Map> getParentChildData() {
		JestClient jestClient = null;
		Search search = null;
		JestResult result = null;
		List<Map> data = new ArrayList<Map>();
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.wildcardQuery("sentence", "*삼성*")).must(QueryBuilders.hasParentQuery("counselor", QueryBuilders.wildcardQuery("name", "*탁명화*"), true).innerHit( new InnerHitBuilder(), true))).size(100).sort("armsoffset", SortOrder.ASC);
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data").build();
			System.out.println(searchSourceBuilder.toString());
			result = jestClient.execute(search);
			
			data = jsonToListMap(result.getJsonObject().get("hits").getAsJsonObject().get("hits"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
		
	}
	
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 7. 17.
	 * @설명	: parent - child type 구조의 JsonElement를 List<Map>으로 변환
	 */
	public static List<Map> jsonToListMap(JsonElement jsonElement) {
		List<Map> result = new ArrayList<Map>();
			for (int i = 0; i < jsonElement.getAsJsonArray().size(); i++) {
				Map value = new HashMap();
				JsonObject jsonObject = new JsonObject();
				jsonObject = (JsonObject) jsonElement.getAsJsonArray().get(i).getAsJsonObject().get("_source");
				value.put("sentence", jsonObject.get("sentence"));
				value.put("date", jsonObject.get("date"));
				value.put("ucid_gkey", jsonObject.get("ucid_gkey"));
				value.put("armsoffset", jsonObject.get("armsoffset"));
				
				jsonObject = (JsonObject) jsonElement.getAsJsonArray().get(i).getAsJsonObject().get("inner_hits").getAsJsonObject().get("counselor").getAsJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray().get(0).getAsJsonObject().get("_source");
				value.put("extension", jsonObject.get("extension"));
				value.put("file_name", jsonObject.get("file_name"));
				value.put("call_time", jsonObject.get("call_time"));
				value.put("name", jsonObject.get("name"));
				value.put("category", jsonObject.get("category"));
				value.put("group", jsonObject.get("group"));
				value.put("direction", jsonObject.get("direction"));
				result.add(value);
			}
		return result;
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 7. 17.
	 * @설명	: Nested(중첩) 필드 키워드의 연관 키워드 ( 연관키워드 ) 
	 */
	public static JSONArray getNestMultiAggreation(){
		JestClient jestClient = null;
		Search search = null;
		JestResult result = null;
		JSONArray data = new JSONArray();
		try {
			jestClient = getJestClient();
			searchSourceBuilder.query(QueryBuilders.rangeQuery("date").from("2017-07-12 00:00:00").to("2017-07-17 23:59:59")).size(0);
			searchSourceBuilder.aggregation(AggregationBuilders
								.nested("nested", "word")
								.subAggregation(
									AggregationBuilders
										.terms("word_result").field("word.words")
										.size(10).subAggregation(
													AggregationBuilders
												.terms("word_result_bottom").field("word.words").size(10)/*.includeExclude(new IncludeExclude(null,"삼성"))*/
										).includeExclude(new IncludeExclude("자전거",null))
									)
								);
			
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			
			result = jestClient.execute(search);
			data = nestedSubAggreation(result.getJsonObject().get("aggregations").getAsJsonObject().get("nested").getAsJsonObject().get("word_result").getAsJsonObject().get("buckets"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	

	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 7. 17.
	 * @설명	: 연관키워드 JSONArray 변환
	 */
	public static JSONArray nestedSubAggreation(JsonElement jsonElement){
		JSONArray result = new JSONArray();
		
		for (int i = 0; i < jsonElement.getAsJsonArray().size(); i++) {
			JsonObject jsonObject = new JsonObject();
			jsonObject.add("key", jsonElement.getAsJsonArray().get(i).getAsJsonObject().get("key"));
			jsonObject.add("value", jsonElement.getAsJsonArray().get(i).getAsJsonObject().get("doc_count"));
			jsonObject.add("buckets", jsonElement.getAsJsonArray().get(i).getAsJsonObject().get("word_result_bottom").getAsJsonObject().get("buckets").getAsJsonArray());
			
			result.add(jsonObject);
		}
		
		return result;
	}
	
	/**
	 * com.bridgetct.core
	 * BridgetctJestSearchMoudel.java
	 *
	 *
	 * @작성자	: 이상민
	 * @작성일	: 2017. 7. 18.
	 * @설명	: 일자별 데이터 건수 ( 콜 트랜드 분석 / 키워드&콜 복합질의 / 연관성분석 )
	 */
	public static List<Map> getDatehistogram() {
		JestClient jestClient = null;
		Search search = null;
		SearchResult result = null;
		List<Map> data = new ArrayList<Map>();
		
		try {
			jestClient = getJestClient();
			
			searchSourceBuilder.query(QueryBuilders.rangeQuery("date").from("2017-07-12 00:00:00").to("2017-07-17 23:59:59")).size(0);
			searchSourceBuilder.aggregation(AggregationBuilders.dateHistogram("result").field("date").dateHistogramInterval(DateHistogramInterval.DAY));
			
			search = new Search.Builder(searchSourceBuilder.toString()).addIndex("join_data")
					.addType("content").build();
			
			result = jestClient.execute(search);
			
			DateHistogramAggregation terms = result.getAggregations().getDateHistogramAggregation("result");
			for (DateHistogram entry :  terms.getBuckets()) {
				Map bucket = new HashMap();
				bucket.put("key", entry.getTimeAsString());
				bucket.put("value", entry.getCount());
				data.add(bucket);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return data;
	}
	
}
