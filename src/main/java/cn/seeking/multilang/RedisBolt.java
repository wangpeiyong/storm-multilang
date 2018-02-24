package cn.seeking.multilang;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author wangpeiyong
 * @Date 2018/1/15 18:26
 */
public class RedisBolt extends BaseBasicBolt {

    private String field;
    private String redisStr;
    private String outputTopic;
    public RedisBolt(String field, String redisStr, String outputTopic) {
        this.field = field;
        this.redisStr = redisStr;
        this.outputTopic = outputTopic;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getStringByField(field);
        JedisPool pool = new JedisPool(new JedisPoolConfig(), redisStr);
        try (Jedis jRedis = pool.getResource()) {
            jRedis.publish(outputTopic, str);
        }
        pool.destroy();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}