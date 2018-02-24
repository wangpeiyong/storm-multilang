package cn.seeking.kafka_examples.word_count;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单词计数
 */
public class WordCountBolt extends BaseBasicBolt {

    /* long: serialVersionUID */
    private static final long serialVersionUID = -799022488579052012L;

    public Logger log = LoggerFactory.getLogger(WordCountBolt.class);

    public static final String field = "result";

    private static Map<String, Integer> counts = new HashMap<String, Integer>();

    private static Lock lock = new ReentrantLock();
    private static volatile boolean isStartTimer = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        // 设置定时任务 只设置一次
        try {
            lock.lock();
            if (!isStartTimer) {
                startTimer();
                isStartTimer = true;
            }
        } finally {
            lock.unlock();
        }
    }

    public void startTimer() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 16); // 控制时
        calendar.set(Calendar.MINUTE, 20);       // 控制分
        calendar.set(Calendar.SECOND, 0);       // 控制秒

        Date time = calendar.getTime();         // 得出执行任务的时间,此处为今天的12：00：00

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                System.out.println("定时任务启动！！");
            }
        }, time, 1000 * 60 * 5);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        // 根据 field 获得上一个 bolt 传递过来的数据
        String word = tuple.getStringByField(WordSplitBolt.field);

        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);

        String result = "word = " + word + ", count = " + count;
        collector.emit(new Values(result));
//        log.info(result);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(field));
    }

}
