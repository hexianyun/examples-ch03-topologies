package countword;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import countword.bolts.WordCounter;
import countword.bolts.WordNormalizer;
import countword.spouts.SignalsSpout;
import countword.spouts.WordReader;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
        
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader",new WordReader());
		builder.setSpout("signals-spout",new SignalsSpout());
		builder.setBolt("word-normalizer", new WordNormalizer())
			.shuffleGrouping("word-reader");
		
		builder.setBolt("word-counter", new WordCounter(), 2)
//			.fieldsGrouping("word-normalizer", new Fields("word"));
//			.allGrouping("signals-spout","signals");
		.directGrouping("word-normalizer");

		
        //Configuration
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(true);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
		Thread.sleep(1000);

		//解决java.io.IOException: Unable to delete file: C:\Users\Mtime\AppData\Local\Temp\5ac71c52-fc17-4b2d-956f-7bb60fb5091a\version-2\log.1
//		cluster.killTopology("Count-Word-Toplogy-With-Refresh-Cache");但是没用

		//控制spout nextTuple循环终止（一个topology会一直运行直到你手动kill掉）
		cluster.shutdown();
	}
}
