package cn.edu.scut.storm.cardio;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import cn.edu.scut.storm.cardio.bolt.CutSTBolt;
import cn.edu.scut.storm.cardio.bolt.HttpClientBolt;
import cn.edu.scut.storm.cardio.bolt.LearnBolt;
import cn.edu.scut.storm.cardio.bolt.PretreatBolt;
import cn.edu.scut.storm.cardio.bolt.WriteFileBolt;
import cn.edu.scut.storm.cardio.spout.ReadFileSpout;
import cn.edu.scut.storm.cardio.spout.ReadRedisSpout;

public class CardioTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
//		builder.setSpout("spout", new ReadFileSpout(), 1);
		builder.setSpout("spout", new ReadRedisSpout(), 1);
		builder.setBolt("pretreat", new PretreatBolt(), 8).setNumTasks(8).shuffleGrouping("spout");
		builder.setBolt("cutST", new CutSTBolt(), 8).setNumTasks(8).shuffleGrouping("pretreat");
		builder.setBolt("learn", new LearnBolt(), 8).setNumTasks(8).shuffleGrouping("cutST");
		builder.setBolt("save", new HttpClientBolt(), 8).setNumTasks(8).shuffleGrouping("learn");
		
		Config conf = new Config();
		conf.setDebug(false);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(1);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else {
			conf.setMaxTaskParallelism(20);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("cardio", conf, builder.createTopology());
		}

	}
}
