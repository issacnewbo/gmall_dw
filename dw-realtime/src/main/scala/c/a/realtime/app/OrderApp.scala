package c.a.realtime.app

import c.a.gmall.constant.GmallConstants
import c.a.realtime.bean.OrderInfo
import c.a.realtime.util.MyKafkaUtil
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER, ssc)

        val orderDStream: DStream[OrderInfo] = inputDStream.map {
            record => {
                val jsonString: String = record.value()

                val orderIndo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                val dateAndTime: Array[String] = orderIndo.create_time.split(" ")
                orderIndo.create_date = dateAndTime(0)
                orderIndo.create_hour = dateAndTime(1).split(":")(0)

                val phoneNum: Array[Char] = orderIndo.consignee_tel.toCharArray
                for (index <- 3 to 6) phoneNum(index) = '*'
                orderIndo.consignee_tel = String.valueOf(phoneNum)

                orderIndo
            }
        }

        orderDStream.foreachRDD {
            rdd => {
                rdd.saveToPhoenix(
                    "ORDER_INFO",
                    Seq(
                        "ID",
                        "PROVINCE_ID",
                        "CONSIGNEE",
                        "ORDER_COMMENT",
                        "CONSIGNEE_TEL",
                        "ORDER_STATUS",
                        "PAYMENT_WAY",
                        "USER_ID",
                        "IMG_URL",
                        "TOTAL_AMOUNT",
                        "EXPIRE_TIME",
                        "DELIVERY_ADDRESS",
                        "CREATE_TIME",
                        "OPERATE_TIME",
                        "TRACKING_NO",
                        "PARENT_ORDER_ID",
                        "OUT_TRADE_NO",
                        "TRADE_BODY",
                        "CREATE_DATE",
                        "CREATE_HOUR"
                    ),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181")
                )
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
