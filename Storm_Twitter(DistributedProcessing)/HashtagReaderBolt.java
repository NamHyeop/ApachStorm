package twitter.test.TwitterTestApp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashtagReaderBolt implements IRichBolt{
	private OutputCollector collector;
	
	private static final Log LOG = LogFactory.getLog(HashtagReaderBolt.class);
	private String hostname;
	
	public HashtagReaderBolt() throws UnknownHostException {
		this.hostname = InetAddress.getLocalHost().getHostName();
	}
	
	public void prepare(Map Conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOG.info("#########[" + hostname + "] Hashtag bolt: prepare method called##########");
	}
	
	int cnt = 0;
	public void execute(Tuple tuple) {
		if(cnt<10) {
			LOG.info("##########[" + hostname + "] Hashtag bolt: exectue method called in 10 times##########" + cnt);
			cnt++;
		}
		Status tweet = (Status) tuple.getValueByField("tweet");
		for(HashtagEntity hashtage : tweet.getHashtagEntities()) {
			LOG.info("##########[" + hostname + "] Hashtag:" + hashtage.getText() + "#########");
			this.collector.emit(new Values(hashtage.getText()));
		}
		
	}
	
	public void cleanup() {
		LOG.info("##########[" + hostname + "] Hashtag bolt: cleanup method called from shutdown topology##########");
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer)  {declarer.declare(new Fields("hashtag"));}
		
	

	@Override
	public Map<String, Object> getComponentConfiguration() {return null;}

	}