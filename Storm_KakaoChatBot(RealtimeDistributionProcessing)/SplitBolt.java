package StormSample.StormSample;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
 //문장을 받은것을 단어들로 다 짤라주는 볼트
public class SplitBolt extends BaseRichBolt { //SentenceSput에 있는 BaseRichBolt를 상속
    private OutputCollector collector;
 
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector; //출력 데이터 정의
    }
 
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));//SentenceSpot에서 받은 Sentence를 Word로 변경
    }
 
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word: words) {
            this.collector.emit(new Values(word));
        }//execute를 통해 입력 받은 tuple을 처리하는 구조  지금 단어 단위로 다 끊어주는 역할을 함
    }
}