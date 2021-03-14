package StormSample.StormSample;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.HashMap;
import java.util.Map;
 //카운팅된 입력 tuple을 받아서 화면에 출력하는 Bolt
public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counter = null;
 //HashMap에 저장되 있던 값을
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counter = new HashMap<String, Long>();
    }
 
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
 
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counter.put(word, count);
        
        System.out.println("[" + word + "]: " + count);
      
        
    }
 
    @Override
    public void cleanup() {                               //Bolt가 정지 시켰을때 실행되는 메소드. 토플로지의 흐름이 계속 데이터를 받으면서 word를 count하다가
        System.out.println("------- FINAL COUNT -------");//정지시키면 각 단어들에 count된 값을 가져오는 메소드
        for (String key: this.counter.keySet()) {
            System.out.println(key + ": " + this.counter.get(key));
        }
        System.out.println("---------------------------");
    }
}
