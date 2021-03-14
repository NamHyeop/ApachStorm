package twitter.test.TwitterTestApp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings("serial")
public class TwitterSampleSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;

	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	String hostname;

	private static final Log LOG = LogFactory.getLog(TwitterSampleSpout.class);

	public TwitterSampleSpout(String consumerkey, String consumerSecret, String accessToken, String accessTokenSecret,
			String[] keyWords) throws UnknownHostException {
		this.consumerKey = consumerkey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		this.hostname = InetAddress.getLocalHost().getHostName();
	}

	public TwitterSampleSpout() {

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			LOG.info("#############[" + InetAddress.getLocalHost().getHostName()
					+ "]Spout: open method called###############");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		StatusListener listener = new StatusListener() {
			
			public void onStatus(Status status) { queue.offer(status);}
			

			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			public void onTrackLimitationNotice(int i) {
			}

			public void onScrubGeo(long l, long l1) {
			}

			public void onException(Exception ex) {
			}
	
			
			public void onStallWarning(StallWarning warning) {
			}
		};

		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);

		_twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		_twitterStream.addListener(listener);
		if (keyWords.length == 0) {
			_twitterStream.sample();
		} else {
			FilterQuery query = new FilterQuery().track(keyWords);
			_twitterStream.filter(query);
		}
	}

	int cnt = 0;

	@Override
	public void nextTuple() {
		Status ret = queue.poll();

		if (ret == null) {
			Utils.sleep(50);
		} else {
			if (cnt < 10) {
				try {
					LOG.info("#############[)" + InetAddress.getLocalHost().getHostName()
							+ "]Spout: nextTuple method called in 10 times########" + cnt);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
				cnt++;
			}
			_collector.emit(new Values(ret));
		}
	}
	
	public void close() {
		try {
			LOG.info("##################[" + InetAddress.getLocalHost().getHostName() + "]Spout: close method called from shutdown topology#######" + cnt);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		_twitterStream.shutdown();
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}
	
	@Override
	public void ack(Object id) {		
	}
	
	@Override
	public void fail(Object id) {
	}
	
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}
