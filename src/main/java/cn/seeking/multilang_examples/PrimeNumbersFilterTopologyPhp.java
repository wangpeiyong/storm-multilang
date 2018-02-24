package cn.seeking.multilang_examples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * @Author wangpeiyong
 * @Date 2018/1/11 16:55
 */
public class PrimeNumbersFilterTopologyPhp {
    private static final String TOPOLOGY_NAME = "prime-numbers-filter-php";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("numbers-generator", new NumberGeneratorSpout(1, 10000));
        builder.setBolt("prime-numbers-filter", new
                PrimeNumbersFilterBolt()).shuffleGrouping("numbers-generator");
        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(TOPOLOGY_NAME, conf,
                    topology);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology(args[0], conf, topology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }

        }
    }
}

class NumberGeneratorSpout extends ShellSpout implements IRichSpout {
    public NumberGeneratorSpout(Integer from, Integer to) {
        super("php", "-f", "primenumbersfilterspolt.php", from.toString(), to.toString());
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

class PrimeNumbersFilterBolt extends ShellBolt implements IRichBolt {
    public PrimeNumbersFilterBolt() {
        super("php", "-f", "primenumbersfilterbolt.php");
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}