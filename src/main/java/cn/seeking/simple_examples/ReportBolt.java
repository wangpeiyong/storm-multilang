package cn.seeking.simple_examples;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


public class ReportBolt extends BaseRichBolt {
    private Map<String, Long> counts;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long count = input.getLongByField("count");
        counts.put(word, count);
    }

    @Override
    public void cleanup() {
        System.out.println("Final output");
        Iterator<Entry<String, Long>> iter = counts.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, Long> entry = iter.next();
            String word = entry.getKey();
            Long count = entry.getValue();
            System.out.println(word + " : " + count);
        }

        super.cleanup();
    }

}
