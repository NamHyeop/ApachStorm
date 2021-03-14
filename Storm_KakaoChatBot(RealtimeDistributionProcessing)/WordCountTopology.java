package StormSample.StormSample;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
//만들었던 Bolt와 Spout을 활용하는 Topology(main문) 
public class WordCountTopology {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) {
		SentenceSpout spout = new SentenceSpout(); //SentenceSpout을 가져와서 사용하는것을 선언
		SplitBolt splitBolt = new SplitBolt();	   //SplitBolt를 가져와서 사용하는것을 선언
		WordCountBolt countBolt = new WordCountBolt();//WordCountBolt를 가져와서 사용하는것을 선언
		ReportBolt reportBolt = new ReportBolt();//ReportBolt를 가져와서사용하는 것을 선언

		TopologyBuilder builder = new TopologyBuilder();//builder 객채 선언

		builder.setSpout(SENTENCE_SPOUT_ID, spout);//SentenceSpoutdp 있는 데이터를 호출
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);//task에서 task로 정보를 전달하는 방식을 shuffleGrouping을 사용
		builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));//task에서 task로 정보를 전달하는 방식을 FiledGrouping을 사용
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID); ////task에서 task로 정보를 전달하는 방식을 GlobalGrouping을 사용
		
		/*local로 사용할 때 사용
		        LocalCluster cluster = new LocalCluster();
		        cluster.submitTopology(TOPOLOGY_NAME, new Config(), builder.createTopology());
		 
		        try { Thread.sleep(1000 * 30); } catch (InterruptedException e) { }
		        cluster.killTopology(TOPOLOGY_NAME);
		        cluster.shutdown();
	
		// Submit topology to cluster
		*/
		
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