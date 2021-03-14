package twitter.test.TwitterTestApp;

import java.net.InetAddress;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TwitterHashtagStorm {
	private static final Log LOG = LogFactory.getLog(TwitterHashtagStorm.class);
	
	public static void main(String[] args) throws Exception {
		String consumerKey = 
				"";
		String consumerSecret = 
				"";
		String accessToken  = 
				"";
		String accessTokenSecret = 
				"";
		
		String hostname = InetAddress.getLocalHost().getHostName();
		
		
		
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments,0,arguments.length);
		
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-spout",
				new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));
		
		builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt()).shuffleGrouping("twitter-spout");
		
		builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt()).fieldsGrouping("twitter-hashtag-reader-bolt",new Fields("hashtag"));
		
		LOG.info("ck= " + consumerKey + "cs =" + consumerSecret + "at =" + accessToken + "ats =" + accessTokenSecret);
		
		
//		LocalCluster cluster = new LocalCluster();
//		LOG.info("##########Topology submit local mode#########");
//		cluster.submitTopology("TwitterHashtagStorm",config,builder.createTopology());
//		Thread.sleep(10000);;
//		
//		LOG.info("#########Topology shutdown#########");
//		cluster.shutdown();
		
		Config conf = new Config();
		conf.setNumWorkers(4); //Works 추가문
		try{
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}catch(AlreadyAliveException ae){
			System.out.println(ae);
		}catch(InvalidTopologyException ie){
			System.out.println(ie);
		} catch (AuthorizationException e) {
			System.out.println(e);
		}
	
	}

}
