package cn.edu.scut.storm.cardio.bolt;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
import org.apache.storm.shade.org.apache.http.entity.StringEntity;
import org.apache.storm.shade.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClients;
import org.apache.storm.shade.org.eclipse.jetty.http.HttpStatus;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientBolt extends BaseRichBolt{
	
	private static final String INSERT_CDG_URL = "http://localhost:8080/cdg/insert";
	private static final String TEST_ID = "testId";
	private static final String CDG_DATA = "cdgData";
	private static final String CDG_RESULTS = "cdgResults";
	private static final String PARA_FFT = "paraFft";
	private static final String PARA_LYA = "paraLya";
	private static final String APPLICATION_JSON = "application/json";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientBolt.class);
	
	private CloseableHttpClient httpClient = null;
	
	public void execute(Tuple tuple) {
		String testId = tuple.getStringByField("testId");
		String cdgData = tuple.getStringByField("WS");
		LOGGER.info("Received CDG data.");
		
		JSONObject object = new JSONObject();
		object.put(TEST_ID, testId);
		object.put(CDG_DATA, cdgData);
		object.put(CDG_RESULTS, "unknown");
		object.put(PARA_FFT, 0.0);
		object.put(PARA_LYA, 0.0);
		
		HttpPost httpPost = new HttpPost(INSERT_CDG_URL);
		StringEntity entity = null;
		try {
			entity = new StringEntity(object.toJSONString());
			entity.setContentEncoding("UTF-8");
			entity.setContentType(APPLICATION_JSON);
			httpPost.setEntity(entity);
			
			LOGGER.info("Inserting CDG in MySQL.");
			HttpResponse response = httpClient.execute(httpPost);
			
			if (response.getStatusLine().getStatusCode() == HttpStatus.OK_200) {
				LOGGER.info("Inserted CDG in MySQL.");
			}
			else {
				LOGGER.info("Insert CDG in MySQL failed.");
			}
			
		} catch (UnsupportedEncodingException e) {
			LOGGER.info("Generate post JSON failed.");
			throw new RuntimeException(e);
		} catch (Exception e) {
			LOGGER.info("Execute http post failed.");
			throw new RuntimeException(e);
		} 
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		httpClient = HttpClients.createDefault();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	
}
