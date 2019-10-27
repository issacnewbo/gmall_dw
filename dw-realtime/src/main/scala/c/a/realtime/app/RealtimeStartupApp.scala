package c.a.realtime.app

import c.a.gmall.constant.GmallConstants
import c.a.realtime.bean.StartUpLog
import c.a.realtime.util.MyKafkaUtil
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeStartupApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("realtime")
        val sc: SparkContext = new SparkContext(sparkConf)
        val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

        val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        //       startupStream.map(_.value()).foreachRDD{ rdd=>
        //         println(rdd.collect().mkString("\n"))
        //       }

        val startupLogDstream: DStream[StartUpLog] = startupStream.map(_.value()).map { log =>
            println(s"log = $log")
            val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
            startUpLog
        }
    }
}