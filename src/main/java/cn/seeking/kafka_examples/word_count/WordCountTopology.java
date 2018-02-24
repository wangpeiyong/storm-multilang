package cn.seeking.kafka_examples.word_count;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WordCountTopology {

    public static Logger log = LoggerFactory.getLogger(WordCountTopology.class);

    private final TopologyBuilder builder;

    public WordCountTopology(Properties configProps) {

        String zkStr = "storm01:2181,storm02:2181,storm03:2181";
        String zkRoot = "/brokers";
        String kafkaStr = "storm01:9092,storm02:9092,storm03:9092";
        String inputTopic_ljc_wordcount = "ljc_wordcount_in";
        String inputTopic_ljc_wordcount2 = "wordcount_in";
        String outputTopic_ljc_wordcount = "ljc_wordcount_out";
        String spoutId_ljc_wordcount = "ljc_wordcount";
        String spoutId_ljc_wordcount2 = "wordcount2";

        log.info("\n inputTopic_ljc_wordcount = " + inputTopic_ljc_wordcount + "\n outputTopic_ljc_wordcount = " + outputTopic_ljc_wordcount + "\n spoutId = " + spoutId_ljc_wordcount);

        // BrokerHosts 接口有 2 个实现类 StaticHosts 和 ZkHosts, ZkHosts 会定时(默认 60 秒)从 ZK 中更新 brokers 的信息(可以通过修改 host.refreshFreqSecs 来设置), StaticHosts 则不会
        // 第二个参数 brokerZkPath 为 zookeeper 中存储 topic 的路径, kafka 的默认配置为 /brokers
        BrokerHosts brokerHosts = new ZkHosts(zkStr, zkRoot);

        // 定义spoutConfig
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, // 第一个参数 hosts 是上面定义的 brokerHosts
                inputTopic_ljc_wordcount,                      // 第二个参数 topic 是该 KafkaSpout 订阅的 topic 名称
                zkRoot,                                        // 第三个参数 zkRoot 是存储消费的 offset(存储在 ZK 中了), 当该 topology 故障重启后会将故障期间未消费的 message 继续消费而不会丢失(可配置)
                spoutId_ljc_wordcount                          // 第四个参数 id 是当前 spout 的唯一标识
        );
        SpoutConfig spoutConfig2 = new SpoutConfig(brokerHosts, // 第一个参数 hosts 是上面定义的 brokerHosts
                inputTopic_ljc_wordcount2,                      // 第二个参数 topic 是该 KafkaSpout 订阅的 topic 名称
                zkRoot,                                        // 第三个参数 zkRoot 是存储消费的 offset(存储在 ZK 中了), 当该 topology 故障重启后会将故障期间未消费的 message 继续消费而不会丢失(可配置)
                spoutId_ljc_wordcount2                         // 第四个参数 id 是当前 spout 的唯一标识
        );

        // 定义 kafkaSpout 如何解析数据, 这里是将 kafka producer 的 send 的数据放入到 String 类型的 str 中输出, str 是 StringSchema 定义的 field, 可以根据业务实现自己的 scheme
        // spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme()); // 自己实现的 Scheme, 输出 field 为 msg
        spoutConfig2.scheme = new SchemeAsMultiScheme(new MessageScheme()); // 自己实现的 Scheme, 输出 field 为 msg

        builder = new TopologyBuilder();

        // 设置 spout: KafkaSpout
        String Spout = KafkaSpout.class.getSimpleName();
        builder.setSpout(Spout, new KafkaSpout(spoutConfig), 4); // topic 的分区数(partitions)最好是 KafkaSpout 的并发度的倍数
        builder.setSpout(spoutId_ljc_wordcount2, new KafkaSpout(spoutConfig2), 4); // topic 的分区数(partitions)最好是 KafkaSpout 的并发度的倍数

        // 设置 一级 bolt
        String Bolt1 = WordSplitBolt.class.getSimpleName();
        builder.setBolt(Bolt1, new WordSplitBolt(), 8) // 并行度 8
                .shuffleGrouping(Spout).shuffleGrouping(spoutId_ljc_wordcount2); // 上一级是 kafkaSpout, 随机分组

        // 设置 二级 bolt
        String Bolt2 = WordCountBolt.class.getSimpleName();
        builder.setBolt(Bolt2, new WordCountBolt(), 12) // 并行度 12
                .fieldsGrouping(Bolt1, new Fields(WordSplitBolt.field)); // 上一级是 WordSplitBolt, 按字段分组

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaStr);
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                .withProducerProperties(producerProps)
                .withTopicSelector(new DefaultTopicSelector(outputTopic_ljc_wordcount))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("", WordCountBolt.field)); // 没有 key, 只传 value

        // 设置 三级 bolt: KafakBolt
        String Bolt3 = KafkaBolt.class.getSimpleName();
        builder.setBolt(Bolt3, kafkaBolt, 8)
                .fieldsGrouping(Bolt2, new Fields(WordCountBolt.field));
    }

    public void submit(String topologyName) throws Exception {
        log.info("############topologyName=" + topologyName);
        Config config = new Config();
        if (topologyName == null || topologyName.isEmpty()) { // 本地运行, 可以看到 log 输出, 用于调试
            topologyName = this.getClass().getSimpleName();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, builder.createTopology());
            try {
                Thread.sleep(1200000); // 20分钟后自动停止 Topology
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                cluster.shutdown();
            }
        } else { // 集群运行
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        }
    }

}
