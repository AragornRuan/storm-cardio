package cn.edu.scut.storm.cardio.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class WriteRedisBolt extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(WriteRedisBolt.class);
	
	private static final String HASH_NAME = "cdgMap";
	private static final String REDIS_HOST = "localhost";
	
	private Jedis jedis = null;
	
	public void execute(Tuple tuple) {
		String testId = tuple.getStringByField("testId");
		String cdgData = tuple.getStringByField("WS");
		
		LOGGER.info("Inserting CDG {} into Redis.", testId);
		jedis.hset(HASH_NAME, testId, cdgData);
		LOGGER.info("Inserted CDG {} into Redis.", testId);
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		jedis = new Jedis(REDIS_HOST);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
