# 这是一个多语言协议的流式计算平台
java直接用就好了，其他语言支持python,nodejs,理论上也支持php（未测试）

## 使用文档：
开始之前需要安装配置好storm集群

### 1.写xxx.json文件，参考word-count-multilang.json和word-count-kafka-multilang.json

平台提供了KafkaSpout、KafkaBolt、RedisBolt
在json文件components下的spouts和bolts下，key以kafka或者redis开头就可以使用
比如
```json
{
  "name":"word-count-multilang",
  "configuration":{
    "topology.debug":true,
    "topology.workers":3
  },
  "components":{
    "spouts":{
      // KafkaSpout
      "kafka_spout":{
        "zk_str":"storm01:2181,storm02:2181,storm03:2181",
        "zk_root":"/brokers",
        "input_topic":"ljc_wordcount_in",
        "field":"word",
        "parallelism_hint":4
      }
    },
    "bolts":{
      "split":{
        "command":"python",
        "path":"splitsentence.py",
        "fields":["word"],
        "grouping":[
          {
            "type":"shuffle",
            "component_id":"kafka_spout"
          }
        ],
        "parallelism_hint":8
      },
      // RedisBolt 用的 jedis.publish(outputTopic, value);
      "redis_bolt":{
        "redis_str":"127.0.0.1",
        "output_topic":"ljc_wordcount_out",
        "field":"word",
        "grouping":[
          {
            "type":"fields",
            "component_id":"split",
            "fields":["word"]
          }
        ],
        "parallelism_hint":8
      }
      /*
      ,"kafka_bolt":{
              "kafka_str":"storm01:9092,storm02:9092,storm03:9092",
              "output_topic":"ljc_wordcount_out",
              "field":"word",
              "grouping":[
                {
                  "type":"fields",
                  "component_id":"split",
                  "fields":["word"]
                }
              ],
              "parallelism_hint":8
          }
      */
    }
  }
}
```
    
### 2.写非java语言的spout和bolt：
比如：randomsentence.js：
```javascript

RandomSentenceSpout.prototype.nextTuple = function (done) {
    var self = this;
    var sentence = this.getRandomSentence();
    var tup = [sentence];
    var id = this.createNextTupleId();
    this.pending[id] = tup;
    //This timeout can be removed if TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS is configured to 100
    setTimeout(function () {
        self.emit({tuple: tup, id: id}, function (taskIds) {
            self.log(tup + ' sent to task ids - ' + taskIds);
        });
        done();
    }, 100);
}
```
比如splitsentence.py：
```Python
import storm
class SplitSentenceBolt(storm.BasicBolt):
    def process(self, tup):
        words = tup.values[0].split(" ")
        for word in words:
          storm.emit([word])

SplitSentenceBolt().run()
```

### 3.把storm打包上传到storm nimbus节点/usr/local 解压
将所有的spout和bolt放在multilang/resources下，将xxx.json文件放在storm-multilang根目录下，然后在storm-multilang根目录下执行
./start-storm.sh word-count-multilang.json 会打jar包提交到storm集群
如果没有执行权限需要加权限
如果报错/bin/bash^M: bad interpreter: No such file or directory，
解决办法:
vi start-storm.sh
:set ff=unix
保存退出就可以了

### 4.查看storm ui

### 碰到问题怎么办？
联系qq 547064775