package twitter.test.TwitterTestApp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class HashtagCounterBolt implements IRichBolt{
	Map<String, Integer> counterMap;
	private OutputCollector collector;
	
private static final Log LOG = LogFactory.getLog(HashtagCounterBolt.class);
String hostname;

public HashtagCounterBolt() throws UnknownHostException{
	this.hostname = InetAddress.getLocalHost().getHostName();
}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
		LOG.info("##########[" + hostname + "]Hashtag counter bolt:[repare method called##########");
	}
	
	int cnt = 0;
	public void execute(Tuple tuple) {
		if(cnt<10) {
			LOG.info("##########[" + hostname + "]Hashtag counter bolt: execute method called in 10 times#########");
			cnt ++;
		}
		
	String key = tuple.getString(0);
	
	if(!counterMap.containsKey(key)) {
		counterMap.put(key, 1);
	}else {
		Integer c = counterMap.get(key) + 1;
		counterMap.put(key, c);
	}
		collector.ack(tuple);
}
	public void cleanup() {
		LOG.info("##########[" +hostname + "]Hashtag counter bolt: cleanup method called from shutdown topology ##########");
		for(Map.Entry<String, Integer> entry:counterMap.entrySet()) {
			LOG.info("##########Result: " + entry.getKey()+" : " + entry.getValue() + "##########");
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("hashtag"));
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
