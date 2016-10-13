package cn.edu.scut.storm.cardio.spout;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.scut.storm.cardio.constants.FileConstants;
import cn.edu.scut.storm.cardio.util.FileOperations;

public class ReadFileSpout extends BaseRichSpout {
	
	SpoutOutputCollector collector;
	LinkedBlockingQueue<Map<String, String>> queue = null;

	private static final String ECG_NAME = "ecgName";
	private static final String ECG_DATA = "ecgData";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileSpout.class);

	public void nextTuple() {
		LOGGER.debug("Before nextTuple()");
		Map<String, String> dataMap = queue.poll();
		if (dataMap == null) {
			LOGGER.debug("data from queue is null.");
			Utils.sleep(50);
		}
		else {
			LOGGER.debug("Before emit.");
			collector.emit(new Values(dataMap.get(ECG_NAME), dataMap.get(ECG_DATA)));
			LOGGER.info("Spout have emitted the tuple.");
		}
		LOGGER.debug("After nextTuple().");
	}
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		queue = new LinkedBlockingQueue<Map<String,String>>(1000);
		
		LOGGER.info("Pushing ECG data into blocking queue.");
		List<File> ecgFiles = FileOperations.listFile(FileConstants.INPUT_DIRECTORY);
		for (File file : ecgFiles) {
			String ecgData = FileOperations.readFile(file);
			LOGGER.debug("ecgData's size is {}.", ecgData.length());
			String filename = file.getName();
			Map<String, String> dataMap = new HashMap<String, String>();
			dataMap.put(ECG_NAME, filename);
			dataMap.put(ECG_DATA, ecgData);
			queue.offer(dataMap);
		}
		LOGGER.info("Having Pushed ECG data into blocking queue.");
		
	}
	
	@Override
	public void ack(Object id) {
		
	}
	
	@Override
	public void fail(Object id) {
		
	}
	
	@Override
	public void close() {
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ecgName", "ecgData"));
	}

}
