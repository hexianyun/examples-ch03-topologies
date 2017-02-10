package countword.bolts;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseRichBolt {

	private OutputCollector collector;
	List<Integer> counterTasks;
	public void cleanup() {}

	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 */
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
//                collector.emit(new Values(word));
				collector.emitDirect(getWordCountTaskId(word),new Values(word));
            }
        }
        // Acknowledge the tuple
        collector.ack(input);
    }

	private Integer getWordCountTaskId(String word) {
		word = word.trim().toUpperCase();
		if(word.isEmpty()){
			return counterTasks.get(0);
		}else{
			return counterTasks.get(word.charAt(0) % counterTasks.size());
		}
	}
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counterTasks = context.getComponentTasks("word-counter");
	}

	/**
	 * The bolt will only emit the field "word" 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
