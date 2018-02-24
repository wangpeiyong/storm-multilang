package cn.seeking.multilang_examples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountTopologyPhp {
    private static final String TOPOLOGY_NAME = "word-count-php";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentencePhp(), 5);

        builder.setBolt("split", new SplitSentencePhp(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountPhp(), 12).fieldsGrouping("split", new Fields("word"));

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

class RandomSentencePhp extends ShellSpout implements IRichSpout {

    public RandomSentencePhp() {
        super("php", "-f", "randomsentencespout.php");
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

class SplitSentencePhp extends ShellBolt implements IRichBolt {

    public SplitSentencePhp() {
        super("php", "-f", "splitsentence.php");
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

class WordCountPhp extends BaseBasicBolt {
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

