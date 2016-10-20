package cn.edu.scut.storm.cardio;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import cn.edu.scut.storm.cardio.bolt.PretreatBolt;
import cn.edu.scut.storm.cardio.bolt.WriteFileBolt;
import cn.edu.scut.storm.cardio.spout.RandomSentenceSpout;
import cn.edu.scut.storm.cardio.spout.ReadFileSpout;

public class CardioTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new ReadFileSpout(), 1);
		
		//builder.setBolt("save", new WriteFileBolt(), 1).shuffleGrouping("spout");
		
		builder.setBolt("pretreat", new PretreatBolt(), 1).shuffleGrouping("spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(1);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else {
			conf.setMaxTaskParallelism(1);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("cardio", conf, builder.createTopology());
		}

	}
}
