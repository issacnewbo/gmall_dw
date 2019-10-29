package c.a.realtime.app

import java.util

import c.a.gmall.constant.GmallConstants
import c.a.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import c.a.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[3]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val inputOrderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER, ssc)
        val inputOrderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
        val inputUserDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_NEW_USER, ssc)

        // 转换kafka_orderInfo topic来的数据的结构
        val orderInfoWithIdDStream: DStream[(String, OrderInfo)] = inputOrderDStream.map(record => {
            val orderInfoJson: String = record.value()
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            // 脱敏
            val dateAndTime: Array[String] = orderInfo.create_time.split(" ")
            val telArr: Array[Char] = orderInfo.consignee_tel.toCharArray
            for (index <- 3 to 6) telArr(index) = '*'
            // 补全字段
            orderInfo.consignee_tel = telArr.toString
            orderInfo.create_date = dateAndTime(0)
            orderInfo.create_hour = dateAndTime(1).split(":")(0)
            (orderInfo.id, orderInfo)
        })

        // 转换kafka_orderDetail topic来的数据的结构
        val orderInfoDetailWithIdDStream: DStream[(String, OrderDetail)] = inputOrderDetailDStream.map(record => {
            val orderDetailJson: String = record.value()
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            (orderDetail.order_id, orderDetail)
        })

        val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithIdDStream.fullOuterJoin(orderInfoDetailWithIdDStream)

        val saleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partitionIter => {
            // 每个分区建立一个redis连接
            val jedis: Jedis = RedisUtil.getJedisClient
            val saleDetailList: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
            // json4需要传入一个隐式转换的参数formats
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

            for ((orderId, (orderInfoOpt, orderDetailOpt)) <- partitionIter) {
                if (orderInfoOpt != None) {
                    val orderInfo: OrderInfo = orderInfoOpt.get

                    // 1.orderDetailOpt 存在，则说明join上了
                    if (orderDetailOpt != None) {
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        saleDetailList.append(new SaleDetail(orderInfo, orderDetail))
                    } else {
                        // 2.orderDetailOpt 不存在，则说明没join上，则去redis中查找缓存的orderDetail
                        val orderDetailSet: util.Set[String] = jedis.smembers("orderDetail:" + orderId)
                        if (orderDetailSet.size() > 0) {
                            val iterator: util.Iterator[String] = orderDetailSet.iterator()
                            if (iterator.hasNext) {
                                val orderDetailJson: String = iterator.next()
                                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                                saleDetailList.append(new SaleDetail(orderInfo, orderDetail))
                            }
                        }
                    }
                    // 将orderInfo写入到redis中，类型为String
                    val orderInfoKey: String = "orderId:" + orderId
                    val orderInfoJson: String = Serialization.write(orderInfo)
                    jedis.set(orderInfoKey, orderInfoJson)
                } else {
                    // orderDetail的值一定存在
                    val orderDetail: OrderDetail = orderDetailOpt.get

                    // 从redis中读取对应orderId的订单进行关联
                    val orderInfoJson: String = jedis.get("orderId:" + orderId)
                    if (orderInfoJson != null) {
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                        saleDetailList.append(new SaleDetail(orderInfo, orderDetail))
                    }

                    // 将orderDetail写入到redis中，类型为Set
                    val orderDetailKey: String = "orderDetail:" + orderId
                    val orderDetailJson: String = Serialization.write(orderDetail)
                    jedis.sadd(orderDetailKey, orderDetailJson)
                    jedis.expire(orderDetailKey, 10 * 60)
                }
            }
            jedis.close()
            saleDetailList.toIterator
        })

        // 将user表从kafka中读出来缓存到redis中
        inputUserDStream.foreachRDD(rdd => {
            rdd.foreachPartition(iterator => {
                val jedis: Jedis = RedisUtil.getJedisClient
                if (iterator.isEmpty) {
                    Thread.sleep(5 * 1000)
                } else {
                    for (record <- iterator) {
                        val userInfoJson: String = record.value()
                        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                        jedis.set("userId:" + userInfo.id, userInfoJson)
                    }
                }
                jedis.close()
            })
        })

        // SaleDetail补全userInfo
        val saleDetailWithUserInfoDStream: DStream[SaleDetail] = saleDetailDStream.mapPartitions(saleDetailIter => {
            val jedis: Jedis = RedisUtil.getJedisClient
            val newIter: Iterator[SaleDetail] = saleDetailIter.map(saleDetail => {
                val userInfoJson: String = jedis.get("userId:" + saleDetail.user_id)
                if (userInfoJson != null) {
                    val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                    saleDetail.mergeUserInfo(userInfo)
                }
                saleDetail
            })
            jedis.close()
            newIter
        })

        // 将saleDetail保存到ES中
        saleDetailWithUserInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(saleDetailIter => {
                val saleDetailWithIdList: List[(String, SaleDetail)] = saleDetailIter.map(saleDetail => {
                    println(saleDetail)
                    (saleDetail.order_detail_id, saleDetail)
                }).toList
                MyEsUtil.insertBulk(GmallConstants.ES_INDEX_SALE_DETAIL, GmallConstants.ES_DEFAULT_TYPE, saleDetailWithIdList)
            })
        })

        ssc.start()
        ssc.awaitTermination()
    }
}