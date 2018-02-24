package cn.seeking.multilang_examples;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class WordCountTopologyNode {
    private static final String TOPOLOGY_NAME = "word-count-node";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceNode(), 5);

        builder.setBolt("split", new SplitSentenceNode(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountNode(), 12).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("report", new ReportBolt(), 12).globalGrouping("count");

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(TOPOLOGY_NAME, conf,
                    builder.createTopology());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }

        }
    }
}

class RandomSentenceNode extends ShellSpout implements IRichSpout {

    public RandomSentenceNode() {
        super("node", "randomsentence.js");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

class SplitSentenceNode extends ShellBolt implements IRichBolt {

    public SplitSentenceNode() {
        super("node", "splitsentence.js");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

class WordCountNode extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}

class ReportBolt extends BaseRichBolt {
    private Map<String, Integer> counts;

    private final String host = "127.0.0.1";
    private final int port = 6379;
    private final String password = "";

    private JedisPool pool;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counts = new HashMap<String, Integer>();
        this.pool = new JedisPool(new JedisPoolConfig(), host, port);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Integer count = input.getIntegerByField("count");
        counts.put(word, count);
        try (Jedis jedis = this.pool.getResource()) {
            jedis.set(word, count.toString());
        }
    }

    @Override
    public void cleanup() {
        System.out.println("Final output");
        Iterator<Map.Entry<String, Integer>> iter = counts.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Integer> entry = iter.next();
            String word = entry.getKey();
            Integer count = entry.getValue();
            System.out.println(word + " : " + count);
        }
        super.cleanup();
    }

}