package cn.seeking.multilang;

import cn.seeking.common.BusinessException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @Author wangpeiyong
 * @Date 2018/1/15 11:26
 */
public class StormMultilang {

    private static Logger logger = LoggerFactory.getLogger(StormMultilang.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            logger.error("topology json file is required");
			throw new IllegalArgumentException("topology json file is required");
		}
        File topologyFile = new File(args[0]);

        JSONObject jsonTopology = null;
        try {
            jsonTopology = (JSONObject) JSONValue.parse(new FileReader(topologyFile));
        } catch (FileNotFoundException e) {
            throw new BusinessException("topology json file not exists, filePath=" + args[0], e);
        }
        if (jsonTopology == null) {
            throw new BusinessException("topology json file format error, please attempt to remove annotations");
        }
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Config conf = new Config();

//        checkJsonFile(jsonTopology);

        String topologyName = (String) jsonTopology.get("name");
        JSONObject components = (JSONObject) jsonTopology.get("components");
        JSONObject spouts = (JSONObject) components.get("spouts");
        JSONObject bolts = (JSONObject) components.get("bolts");

        for(Map.Entry<String, Object> spout : (Set<Map.Entry<String, Object>>)spouts.entrySet()) {
            String componentId = spout.getKey();
            if (componentId.startsWith("kafka")) {
                JSONObject configProps = (JSONObject) spout.getValue();
                String zkStr = (String) configProps.get("zk_str");
                String zkRoot = (String) configProps.get("zk_root");
                String inputTopic = (String) configProps.get("input_topic");
                Long parallelismHint = (Long) configProps.get("parallelism_hint");
                String field = (String) configProps.get("field");

                // BrokerHosts 接口有 2 个实现类 StaticHosts 和 ZkHosts, ZkHosts 会定时(默认 60 秒)从 ZK 中更新 brokers 的信息(可以通过修改 host.refreshFreqSecs 来设置), StaticHosts 则不会
                // 第二个参数 brokerZkPath 为 zookeeper 中存储 topic 的路径, kafka 的默认配置为 /brokers
                BrokerHosts brokerHosts = new ZkHosts(zkStr, zkRoot);

                // 定义spoutConfig
                SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, // 第一个参数 hosts 是上面定义的 brokerHosts
                        inputTopic,                      // 第二个参数 topic 是该 KafkaSpout 订阅的 topic 名称
                        zkRoot,                                        // 第三个参数 zkRoot 是存储消费的 offset(存储在 ZK 中了), 当该 topology 故障重启后会将故障期间未消费的 message 继续消费而不会丢失(可配置)
                        componentId                          // 第四个参数 id 是当前 spout 的唯一标识
                );

                // 定义 kafkaSpout 如何解析数据, 这里是将 kafka producer 的 send 的数据放入到 String 类型的 str 中输出, str 是 StringSchema 定义的 field, 可以根据业务实现自己的 scheme
                // spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme(field)); // 自己实现的 Scheme, 输出 field 为 msg

                // 设置 spout: KafkaSpout
                topologyBuilder.setSpout(componentId, new KafkaSpout(spoutConfig), parallelismHint); // topic 的分区数(partitions)最好是 KafkaSpout 的并发度的倍数
            }else {
                JSONObject componentDefinition = (JSONObject) spout.getValue();
                String command = (String) componentDefinition.get("command");
                List<String> fields = (List<String>) componentDefinition.get("fields");
                topologyBuilder.setSpout(componentId, new ScriptSpout(command, (String) componentDefinition.get("path"), fields), (Long)componentDefinition.get("parallelism_hint"));
            }
        }
        for(Map.Entry<String, Object> bolt : (Set<Map.Entry<String, Object>>)bolts.entrySet()) {
            String componentId = bolt.getKey();
            JSONObject componentDefinition = (JSONObject) bolt.getValue();
            BoltDeclarer boltDeclarer = null;
            if (componentId.startsWith("kafka")) {
                String kafkaStr = (String) componentDefinition.get("kafka_str");
                String outputTopic = (String) componentDefinition.get("output_topic");
                Long parallelismHint = (Long) componentDefinition.get("parallelism_hint");
                String key = (String) componentDefinition.get("key");
                String value = (String) componentDefinition.get("value");

                Properties producerProps = new Properties();
                producerProps.put("bootstrap.servers", kafkaStr);
                producerProps.put("acks", "all");
                producerProps.put("key.serializer", StringSerializer.class.getName());
                producerProps.put("value.serializer", StringSerializer.class.getName());

                KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
                        .withProducerProperties(producerProps)
                        .withTopicSelector(new DefaultTopicSelector(outputTopic))
                        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>(key, value));

                // 设置  KafakBolt
                boltDeclarer = topologyBuilder.setBolt(componentId, kafkaBolt, parallelismHint);
            } else if (componentId.startsWith("redis")) {
                String field = (String) componentDefinition.get("field");
                String redisStr = (String) componentDefinition.get("redis_str");
                String outputTopic = (String) componentDefinition.get("output_topic");
                boltDeclarer = topologyBuilder.setBolt(componentId, new RedisBolt(field, redisStr, outputTopic), (Long)componentDefinition.get("parallelism_hint"));
            } else {
                String command = (String) componentDefinition.get("command");
                List<String> fields = (List<String>) componentDefinition.get("fields");
                boltDeclarer = topologyBuilder.setBolt(componentId, new ScriptBolt(command, (String) componentDefinition.get("path"), fields), (Long)componentDefinition.get("parallelism_hint"));
            }
            JSONArray groupings = (JSONArray) componentDefinition.get("grouping");
            for (Object grouping : groupings) {
                String groupingComponentId = (String) ((JSONObject) grouping).get("component_id");
                String groupingType = (String) ((JSONObject) grouping).get("type");

                if (groupingType.equals("shuffle")) {
                    boltDeclarer.shuffleGrouping(groupingComponentId);
                }

                if (groupingType.equals("global")) {
                    boltDeclarer.globalGrouping(groupingComponentId);
                }

                if (groupingType.equals("fields")) {
                    List<String> groupingFields = (List<String>) ((JSONObject) grouping).get("fields");
                    boltDeclarer.fieldsGrouping(groupingComponentId, new Fields(groupingFields));
                }
            }
        }

        conf.putAll((Map<String, Object>) jsonTopology.get("configuration"));

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf,
                    topologyBuilder.createTopology());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            try {
                StormSubmitter.submitTopology(topologyName, conf, topologyBuilder.createTopology());
            } catch (AlreadyAliveException e) {
                throw new BusinessException("the topology already exists, please run \"storm kill " + topologyName + "\" first", e);
            } catch (InvalidTopologyException e) {
                throw new BusinessException(e);
            } catch (AuthorizationException e) {
                throw new BusinessException(e);
            }
        }
    }

    private static void checkJsonFile(JSONObject jsonTopology) {
        try {
            String topologyName = (String) jsonTopology.get("name");
            JSONObject components = (JSONObject) jsonTopology.get("components");
            JSONObject spouts = (JSONObject) components.get("spouts");
            JSONObject bolts = (JSONObject) components.get("bolts");

            // TODO
        } catch (ClassCastException e) {
            throw new BusinessException("arguments type error: ", e);
        }

    }
}
