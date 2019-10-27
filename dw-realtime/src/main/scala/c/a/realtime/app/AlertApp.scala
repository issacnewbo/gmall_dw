package c.a.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import c.a.gmall.constant.GmallConstants
import c.a.realtime.bean.{CouponAlertInfo, EventInfo}
import c.a.realtime.util.{MyEsUtil, MyKafkaUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AlertApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coupon_alert_app")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

        val eventInfoDStream: DStream[EventInfo] = inputDStream.map(
            record => {
                val info: EventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
                val ts: Long = info.ts
                info.logDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
                info.logHour = new SimpleDateFormat("HH").format(new Date(ts))
                info
            }
        )

        val eventInfoWindowDStream: DStream[EventInfo] = eventInfoDStream.window(Minutes(5), Seconds(10))

        val checkCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = eventInfoWindowDStream
                .map(eventInfo => (eventInfo.mid, eventInfo))
                .groupByKey()
                .map {
                    case (mid, eventInfoIter) => {
                        val uids: util.HashSet[String] = new util.HashSet[String]()
                        val itemIds: util.HashSet[String] = new util.HashSet[String]()
                        val events: util.ArrayList[String] = new util.ArrayList[String]()
                        var flag: Boolean = true

                        for (eventInfo: EventInfo <- eventInfoIter) {
                            if ("coupon" == eventInfo.evid) {
                                uids.add(eventInfo.uid)
                                itemIds.add(eventInfo.itemid)
                            } else if ("clickItem" == eventInfo.evid) {
                                flag = false
                            }
                            events.add(eventInfo.evid)
                        }
                        (flag, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
                    }
                }

        val filteredDstream: DStream[(Boolean, CouponAlertInfo)] = checkCouponAlertDStream.filter(_._1)

        val alertInfoWithIdDstream: DStream[(String, CouponAlertInfo)] = filteredDstream.map {
            case (flag, couponAlertInfo) => {
                val minutes: Long = couponAlertInfo.ts / 1000 / 60
                val id: String = couponAlertInfo.mid + "_" + minutes
                (id, couponAlertInfo)
            }
        }

        alertInfoWithIdDstream.foreachRDD{
            rdd => {
                rdd.foreachPartition {
                    alterInfoIter => MyEsUtil.insertBulk(GmallConstants.ES_INDEX_COUPON,GmallConstants.ES_DEFAULT_TYPE, alterInfoIter.toList)
                }
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}