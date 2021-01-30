package com.pcy.bigdata.spark.streaming

import java.util.Random

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * [root@VM-8-9-centos kafka_2.13-2.6.0]# bin/kafka-topics.sh --zookeeper 49.232.218.99:2181 --create --replication-factor 1 --partitions 3 --topic testTopic
 * [root@VM-8-9-centos bin]# ./kafka-console-producer.sh --broker-list 49.232.218.99:9092 --topic testTopic
 */
object SparkStreaming_Kafka {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "49.232.218.99:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "test-consumer-group",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )


        // ConsumerRecord[key, value]
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            // 消费策略
            ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara)
        )

        // 获取value值
        kafkaDataDS.map(_.value()).print()


        ssc.start()
        ssc.awaitTermination()
    }

}
