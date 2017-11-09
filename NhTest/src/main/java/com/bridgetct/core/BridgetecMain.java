package com.bridgetct.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bitbucket.eunjeon.seunjeon.Analyzer;
import org.bitbucket.eunjeon.seunjeon.LNode;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONObject;

public class BridgetecMain {
	
	public static String[] KEYWORD_ROOT = null;
	public static List<Map> SENTIKEYWORD = null;
	
	public static void main(String[] args) throws UnknownHostException {
		long startTime = System.currentTimeMillis();
		long endTime = 0;
		System.out.println("=================================================");
		System.out.println("프로그램 시작");
		System.out.println("=================================================");
		
		Settings settings = Settings.builder()
				.put("cluster.name", "elasticsearch")
				//x-pack 설치시
				//.put("xpack.security.user", "transport_client_user:changeme")
				.put("client.transport.ignore_cluster_name", true)
				.build();
		
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("100.100.107.82"), 9300));
		try {
			
			List<JSONObject> jsonResult = excelRead();
			boolean check = true;
			try {
				String ucid = "";
				String uuid = "";
				IndexResponse indexResponse = null;
				
				Random random = new Random();
				int cycle = 1;
				String[] extension = new String[] { "26713", "11047", "11589", "71530", "11204", "11294", "12423", "53425", "23534", "23663"};
				String[] names = new String[] { "탁명화", "최희경", "강한모", "이혜진", "박진희", "배재민", "김민희", "김희선", "이은지", "박미성"};
				String[] group = new String[] {"용산", "광주"};
				int[] call_time = new int[] {30, 44, 55, 66, 78, 84, 94, 102, 115, 123, 135, 147, 156, 168, 177, 183, 192, 201, 214, 224, 257, 300, 342, 500};
				
//				while (check) {
						
					System.out.println("현재 " + cycle +"번째 돌고 있습니다.");
					for (int i = 0; i < jsonResult.size(); i++) {
						Date date = new Date();
						int num = 1;
						Calendar cal = Calendar.getInstance();
						cal.setTime(date);
						cal.add(Calendar.DATE, - num);
						List wordList = new ArrayList();
						String [] word = null;
						String TENANT_ID = "bridgetec";
						/* Arraoy 데이터 입력 S */
						JSONObject job = new JSONObject();
						if(!ucid.equals(jsonResult.get(i).get("ucid_gkey").toString().replace(".", "") + "" +num)){
							JSONObject counselor = new JSONObject();
							
							uuid = UUID.randomUUID().toString();
							ucid = jsonResult.get(i).get("ucid_gkey").toString().replace(".", "") + "" +num;
							//통화 일시
							counselor.put("CALL_DATE", new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(cal.getTime()));
							//통화 시간
							counselor.put("CALL_DUR", call_time[random.nextInt(10)]);
							//내선번호
							counselor.put("DN_NO",  extension[random.nextInt(10)]);
							//콜센터
							counselor.put("CENTER_NAME", group[random.nextInt(2)]);
							//상담원
							counselor.put("AGENT_NAME", names[random.nextInt(10)]);
							//방향
							counselor.put("CALL_DIR", "1");
							//테런트 아이디
							counselor.put("TENANT_ID", TENANT_ID);
							//고유번호
							counselor.put("UCID_GKEY", jsonResult.get(i).get("ucid_gkey").toString().replace(".", "") + "" +num);
							counselor.put("GROUP_NAME", "1");
							
							
							client.prepareIndex("raw_data_2017_3", "TB_CA_CALLINFO")
									.setId(jsonResult.get(i).get("ucid_gkey").toString().replace(".", "") + "" +num)
									.setSource(counselor)
									.execute()
									.actionGet();
						}
						Random rand = new Random();
						float minX = 20.0f;
						float maxX = 70.0f;
						for (LNode node : Analyzer.parseJava(jsonResult.get(i).get("word").toString())) {
							String KEYWORD_CLASS = node.morpheme().copy$default$5().apply(0);
							if(node.morpheme().surface().length() > 1){
								if(KEYWORD_CLASS.equals("NNG") || KEYWORD_CLASS.equals("NNP") || KEYWORD_CLASS.equals("NNB") || KEYWORD_CLASS.equals("NNBC") || KEYWORD_CLASS.equals("VV") || KEYWORD_CLASS.equals("VA")){
									JSONObject jsonObject = new JSONObject();
									jsonObject.put("WORDS", node.morpheme().surface().toString());
									jsonObject.put("WORDS_SIZE", node.morpheme().surface().toString().length());
									jsonObject.put("TEMPO", rand.nextFloat() * (maxX - minX) + minX);
									jsonObject.put("PITCH", rand.nextFloat() * (maxX - minX) + minX);
									jsonObject.put("ENERGY", rand.nextFloat() * (maxX - minX) + minX);
									jsonObject.put("KEYWORD_CLASS", KEYWORD_CLASS);
									wordList.add(jsonObject);
								}
							}
				        }
						job.put("UCID_GKEY", jsonResult.get(i).get("ucid_gkey").toString().replace(".", "") + "" +num);
						job.put("ARMSOFFSET", jsonResult.get(i).get("armsoffset").toString().replace(".0", ""));
						job.put("SENTENCE", jsonResult.get(i).get("word"));
						job.put("WORD", wordList);
						job.put("RXTX_KIND", "9");
						job.put("CALL_DATE", new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(cal.getTime()));
						String parent = jsonResult.get(i).get("ucid_gkey").toString().replace(".", "") + "" +num;
						client.prepareIndex("raw_data_2017_3", "TB_CA_RESULTSENTENCE")
//								.setId(jsonData) 
								.setParent(parent)
								.setSource(job)
								.execute()
								.actionGet();
						client.prepareIndex("raw_data_2017_3", "TB_CA_RESULTWORDS")
//								.setId(jsonData) 
								.setParent(parent)
								.setSource(job)
								.execute()
								.actionGet();
						/* Array 데이터 입력 E */
						
						if(SENTIKEYWORD == null){
							getSentikeyword();
						}
						
						if (job.get("SENTENCE") != null) {
							if(StringUtils.startsWithAny(job.get("SENTENCE").toString(), KEYWORD_ROOT)){
								for (int j = 0; j < SENTIKEYWORD.size(); j++) {
									int cnt = 0;
									cnt = StringUtils.countMatches(job.get("SENTENCE").toString(), SENTIKEYWORD.get(j).get("KEYWORD_ROOT").toString());
									if (cnt > 0) {
										JSONObject sentimentalJson = new JSONObject();
										sentimentalJson.put("SENTI_KEYWORD", SENTIKEYWORD.get(j).get("SENTI_KEYWORD"));		//대표감성
										sentimentalJson.put("KEYWORD_ROOT", SENTIKEYWORD.get(j).get("KEYWORD_ROOT"));		//세부감성
										sentimentalJson.put("COUNT", cnt);													//카운트
										sentimentalJson.put("CALL_DATE", job.get("CALL_DATE"));						//입력일시
										sentimentalJson.put("PRAISE_CLAIM", SENTIKEYWORD.get(j).get("PRAISE_CLAIM"));		//긍,부정
										sentimentalJson.put("SENTENCE", job.get("SENTENCE"));						//문장
										sentimentalJson.put("UCID_GKEY", job.get("UCID_GKEY"));						//상담 고유키
										sentimentalJson.put("TENANT_ID", TENANT_ID);						//테런트 아이디
										
										client.prepareIndex("sentiment_data", "ANALYSIS")
										.setSource(sentimentalJson)
										.execute()
										.actionGet();
									}
								}
							}
						}
						
					}
					cycle++;
//				}
			} catch (Exception e) {
				check = false;
				System.out.println("===============================에러발생===============================");
				System.out.println(e);
				System.out.println("===============================에러발생===============================");
			}
			endTime = System.currentTimeMillis();
			
		} catch (Exception e) {	
			System.out.println("error : " + e);
		} finally {
			client.close();
	        System.out.println("##  소요시간(초.0f) : " + ( endTime - startTime )/1000.0f +"초"); 
	        System.out.println("=========================== 완료 ========================");
		}
		
	}
	
	
	public static List<JSONObject> excelRead() {
		List<JSONObject> jsonResult = new ArrayList<JSONObject>();
		try {
		FileInputStream	fis = new FileInputStream(System.getProperty("user.dir")+"\\data.xlsx");
//		FileInputStream	fis = new FileInputStream("/bridgetec/elastic/data.xlsx");
		
		XSSFWorkbook workbook=new XSSFWorkbook(fis);
		int rowindex=0;
		int columnindex=0;
		XSSFSheet sheet=workbook.getSheetAt(0);
		int rows=sheet.getPhysicalNumberOfRows();
		for(rowindex=0;rowindex<rows;rowindex++){
		    XSSFRow row=sheet.getRow(rowindex);
		    if(row !=null){
		        int cells=row.getPhysicalNumberOfCells();
		        JSONObject job = new JSONObject();
		        for(columnindex=0;columnindex<=cells;columnindex++){
		            XSSFCell cell=row.getCell(columnindex);
		            String value="";
		            if(cell==null){
			                continue;
			            }else{
		                switch (cell.getCellType()){
		                case XSSFCell.CELL_TYPE_FORMULA:
		                    value=cell.getCellFormula();
		                    break;
		                case XSSFCell.CELL_TYPE_NUMERIC:
		                    value=cell.getNumericCellValue()+"";
		                    break;
		                case XSSFCell.CELL_TYPE_STRING:
		                    value=cell.getStringCellValue()+"";
		                    break;
		                case XSSFCell.CELL_TYPE_BLANK:
		                    value=cell.getBooleanCellValue()+"";
		                    break;
		                case XSSFCell.CELL_TYPE_ERROR:
		                    value=cell.getErrorCellValue()+"";
		                    break;
		                }
		            }
		           
		            if(columnindex == 0){
		            	job.put("ucid_gkey", value);
		            } else if (columnindex == 1){
		            	job.put("armsoffset", value);
		            } else if (columnindex == 2){
		            	job.put("word", value);
		            }
		        }
		        jsonResult.add(job);
		    }
		}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jsonResult;
	}
	
	public static synchronized void getSentikeyword () {
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "";
		List<Map> result = new ArrayList<Map>();
		List<String> keywordResult = new ArrayList<String>();
		try {
			Class.forName("com.tmax.tibero.jdbc.TbDriver");
			Connection connection = DriverManager.getConnection("jdbc:tibero:thin:@100.100.107.89:8629:CatchAll", "icatch", "icatch");
			stmt = connection.createStatement();
			sql +="SELECT";
			sql +="    SENTI_KEYWORD as SENTI_KEYWORD";
			sql +="    , KEYWORD_ROOT as KEYWORD_ROOT";
			sql +="    , PRAISE_CLAIM as PRAISE_CLAIM";
			sql +=" FROM";
			sql +="    TB_CA_SENTIKEYWORD";
			sql +=" WHERE";
			sql +="    ACTIVE_YN = '1'";
			rs = stmt.executeQuery(sql);
			int i = 0;
			while(rs.next()) {
				Map map = new HashMap();
				map.put("SENTI_KEYWORD", rs.getObject("SENTI_KEYWORD"));
				map.put("KEYWORD_ROOT", rs.getObject("KEYWORD_ROOT"));
				map.put("PRAISE_CLAIM", rs.getObject("PRAISE_CLAIM"));
				keywordResult.add(map.get("KEYWORD_ROOT").toString());
				result.add(map);
				i++;
			}
			SENTIKEYWORD = result;
			KEYWORD_ROOT = keywordResult.toArray(new String[keywordResult.size()]);
		} catch (SQLException e) {
			System.out.println("error");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) rs.close();
				if(stmt != null) stmt.close();
			} catch (SQLException e1) {
				System.out.println("error");
			}
		}
	}
}
