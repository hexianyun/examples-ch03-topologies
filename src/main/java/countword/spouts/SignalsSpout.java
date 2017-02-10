package countword.spouts;

import java.time.LocalTime;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 单实例
 */
public class SignalsSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	int i=0;

	@Override
	public void nextTuple() {
		//全部数据流组，为每个接收数据的实例(看word-counter的实例)复制一份元组副本
		collector.emit("signals",new Values("refreshCache"));
		System.out.println("SignalsSpout nextTuple num="+(++i)+"-"+ LocalTime.now());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
		System.out.println("SignalsSpout nextTuple end."+ LocalTime.now());
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("signals",new Fields("action"));
	}

}
