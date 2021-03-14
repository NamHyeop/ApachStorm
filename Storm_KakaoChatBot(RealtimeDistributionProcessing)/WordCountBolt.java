package StormSample.StormSample;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
 //단어들로 짤라진 데이터를 카운팅하기 위한 Bolt
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String, Long> counter = null; //HashMapdl word를 카운팅한 값을 계속 유지
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counter = new HashMap<String, Long>();
    }
 
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count")); //count한 값을 실시간으로 계속 출력
    }
 
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counter.get(word); //word가 들어오면 그 단어를 가져와서
        count = count == null ? 1L : count + 1; //count해서 밑으로 넘겨주고
        this.counter.put(word, count);	//단어에 맞는 숫자 카운터 1증가
        this.collector.emit(new Values(word, count));
    }
}