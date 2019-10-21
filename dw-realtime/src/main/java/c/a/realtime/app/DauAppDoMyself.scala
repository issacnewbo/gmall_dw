package c.a.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import c.a.gmall.constant.GmallConstants
import c.a.realtime.bean.StartUpLog
import c.a.realtime.util.{MyKafkaUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauAppDoMyself {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("dau_myself").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        val startuplogDstream: DStream[StartUpLog] = inputDStream.map(_.value()).map {
            startupStr => {
                val startupLog: StartUpLog = JSON.parseObject(startupStr, classOf[StartUpLog])
                val date: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
                val dateAndHour: Array[String] = date.split(" ")
                startupLog.logDate = dateAndHour(0)
                startupLog.logHour = dateAndHour(1)
                startupLog
            }
        }

        // 批次间过滤
        val filteredDstream: DStream[StartUpLog] = startuplogDstream.transform {
            startUpLogRDD => {

                val redisCli: Jedis = RedisUtil.getJedisClient
                val midSet: util.Set[String] = redisCli.smembers("dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
                redisCli.close()

                println(s"过滤前 日志数量：${midSet.size()}")

                val midSetBroadCast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

                val filteredStartUpLog: RDD[StartUpLog] = startUpLogRDD.filter {
                    eachLog => {
                        val midSet: util.Set[String] = midSetBroadCast.value
                        !midSet.contains(eachLog.mid)
                    }
                }

                println(s"过滤后 日志数量：${filteredStartUpLog.count()}")

                filteredStartUpLog
            }
        }

        val finalFilteredDEtream: DStream[StartUpLog] = filteredDstream.map(startUpLog => (startUpLog.mid, startUpLog)).reduceByKey {
            case (log1, log2) => log1
        }.map(_._2)

        finalFilteredDEtream.foreachRDD {
            eachRDD => {
                eachRDD.foreachPartition {
                    LogIter => {
                        val redisCli: Jedis = RedisUtil.getJedisClient
                        for (eachLog <- LogIter) {
                            redisCli.sadd("dau:" + eachLog.logDate, eachLog.mid)
                        }
                        redisCli.close()
                    }
                }
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
