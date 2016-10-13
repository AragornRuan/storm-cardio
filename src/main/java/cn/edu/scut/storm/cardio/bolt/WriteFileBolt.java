package cn.edu.scut.storm.cardio.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.scut.storm.cardio.constants.FileConstants;
import cn.edu.scut.storm.cardio.util.FileOperations;

public class WriteFileBolt extends BaseBasicBolt {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WriteFileBolt.class);

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String ecgName = tuple.getString(0);
		String ecgData = tuple.getString(1);
		
		StringBuilder filePath = new StringBuilder();
		filePath.append(FileConstants.OUTPUT_DIRECTORY).append("/").append(ecgName);
		LOGGER.info("Saving file {}.", filePath.toString());
		FileOperations.saveFile(filePath.toString(), ecgData);
		LOGGER.info("Save file {} successfully.");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
