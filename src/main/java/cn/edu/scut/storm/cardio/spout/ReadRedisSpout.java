package cn.edu.scut.storm.cardio.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class ReadRedisSpout extends BaseRichSpout{

	private static final Logger LOGGER = LoggerFactory.getLogger(ReadRedisSpout.class);
	
	private static final String LIST_NAME = "testId";
	private static final String HASH_NAME = "ecgMap";
	private static final String REDIS_HOST = "localhost";

	
	SpoutOutputCollector collector;
	private Jedis jedis = null;
		

	public void nextTuple() {
		String testId = jedis.lpop(LIST_NAME);
		if (testId == null) {
			Utils.sleep(50);
		}
		else {
			LOGGER.info("Emitting ECG {}.", testId);
			String ecgData = jedis.hget(HASH_NAME, testId);
			collector.emit(new Values(testId, ecgData));
			LOGGER.info("Emitted ECG {}.", testId);
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		jedis = new Jedis(REDIS_HOST);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("testId", "ecgData"));
	}

}
