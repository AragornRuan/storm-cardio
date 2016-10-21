package cn.edu.scut.storm.cardio.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.scut.storm.cardio.constants.FileConstants;
import cn.edu.scut.storm.cardio.util.FileOperations;

public class WriteFileBolt extends BaseRichBolt {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WriteFileBolt.class);

	public void execute(Tuple tuple) {
		String fileName = tuple.getStringByField("filename");
		String CDG = tuple.getStringByField("WS");
		
		StringBuilder filePath = new StringBuilder();
		filePath.append(FileConstants.OUTPUT_DIRECTORY).append("/").append(fileName);
		LOGGER.info("Saving file {} with sfreq: {}.", filePath.toString());
		FileOperations.saveFile(filePath.toString(), CDG);
		LOGGER.info("Save file {} successfully.", filePath.toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}


	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

}
