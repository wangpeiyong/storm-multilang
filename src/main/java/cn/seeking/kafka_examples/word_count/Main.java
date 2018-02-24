package cn.seeking.kafka_examples.word_count;

import java.io.IOException;
import java.util.Properties;

/**
 * @author wangpeiyong
 *
 * 这是一个 kafka -> storm -> kafka demo 程序
 * storm 提交时指定这个 Main 类为入口, 则只运行此包下的拓扑, 可用于调试(无参数则为本地, 有拓扑名参数则为集群)
 * 调试没有问题则可配置到 Main 中, storm 提交时指定 Main 类为入口, 则运行所有配置了的拓扑
 */
public class Main {

    public static void main(String[] args) throws Exception {

        Properties configProps = new Properties();
        try {
            configProps.load(Main.class.getClassLoader().getResourceAsStream("sysConfig.properties"));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("ERROR 1: no Config file.");
            return;
        }

        // 配置 WordCountTopologyPython
        WordCountTopology wordCountTopology = new WordCountTopology(configProps);

        if (args == null || args.length == 0) {
            wordCountTopology.submit(null); // 无参则本地模式
        } else {
            wordCountTopology.submit(args[0]); // 有参则集群模式
        }
    }
}
