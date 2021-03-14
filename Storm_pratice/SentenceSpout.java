package StormSample.StormSample;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class SentenceSpout extends BaseRichSpout{
	private SpoutOutputCollector collector;
	private int index = 0;
	private final String[] sentences = {
			"my dog has fleas",
			"i like cold beveages",
			"the dog ate my bomework",
			"don't have a cow man",
			"i don't think i like fleas"
	};
	
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("sentence"));
	}
	
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if(index >= sentences.length) {
			index = 0;
		}
		try {
			Thread.sleep(5);
			}
		catch(InterruptedException e)
		{
		
		}
	}

}

