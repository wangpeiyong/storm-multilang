package cn.seeking.kafka_examples.data_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DataProducer {
    private static Random random = new Random(93285);
    private static Producer<String, String> producer;

    public static void main(String args[]) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        String[] arrays = new String[]{"hello world", "hehe hh ee", "haha ha", "who whe", "are 11", "you"};

        while (true) {
            int randomNum = random.nextInt(arrays.length);
            String sendStr = arrays[randomNum];
            producer.send(new ProducerRecord<String, String>("wordcount_in", sendStr));
            System.out.println("发送：" + sendStr);
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        while (true) {
//            int choise = 0 + random.nextInt(10000);
//            switch (choise) {
//                case 0:
//                    tdata = createRandom(-30, -20);
//                    hdata = createRandom(0.0, 4.9);
//                    break;
//                case 9999:
//                    tdata = createRandom(60, 70);
//                    hdata = createRandom(96.0, 100.0);
//                    break;
//                default:
//                    tdata = createRandom(-19, 59);
//                    hdata = createRandom(5.0, 95.9);
//                    break;
//            }
//            producer.send(new ProducerRecord<String, String>("ljc_wordcount_in", "temper:" + tdata + "," + "humi:" + hdata));
//            long endTime = System.nanoTime();
//            count++;
//            int a = (int) ((endTime - startTime) * Math.pow(10, -9));
//            if (a == 1) {
//                System.out.println("每秒发送：" + count + "条数据");
//                count = 0;
//                startTime = System.nanoTime();
//            }
//        }

    }

    private static int createRandom(int min, int max) {
        return min + random.nextInt(max - min);
    }

    private static double createRandom(double min, double max) {
        if (min == 0) {
            return max - random.nextDouble();
        }
        return max - random.nextDouble() * min;

    }

}