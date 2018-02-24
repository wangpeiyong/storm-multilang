package cn.seeking.simple_examples;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(
                SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID,
                new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(
                COUNT_BOLT_ID);

        Config conf = new Config();

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
