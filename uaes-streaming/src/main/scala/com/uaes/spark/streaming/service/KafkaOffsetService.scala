package com.uaes.spark.streaming.service

import java.util.concurrent.atomic.AtomicReference

import com.uaes.spark.streaming.config.ConfigManager
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ZKGroupTopicDirs
import org.apache.commons.lang.StringUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
/**
  * Created by mzhang on 2017/10/16.
  */
class KafkaOffsetService(offsetRanges: AtomicReference[Array[OffsetRange]], zkService: ZkService) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[KafkaOffsetService])

  private def getLeaderAndPartition(topics: Set[String], brokerHost: String, port: Int): Map[Int, String] = {
    val rq = new TopicMetadataRequest(topics.toSeq, 0)
    val leaderConsumer = new SimpleConsumer(brokerHost, port, 600, 1024, "getLeader")
    val rs = leaderConsumer.send(rq)
    val metadataOption = rs.topicsMetadata.headOption
    val partitions = metadataOption match {
      case Some(mo) => mo.partitionsMetadata.map(mo => (mo.partitionId, mo.leader.get.host)).toMap[Int, String]
      case None => Map[Int, String]()
    }
    partitions
  }

  private def getLastOffset(group: String, topic: Set[String], path: String, brokerHost: String, port: Int): Map[TopicAndPartition, Long] = {

    val leaderWithPartition = getLeaderAndPartition(topic, brokerHost, port)
    leaderWithPartition.map(f => {
      val partition = f._1
      val leader = f._2
      val partitionOffset = zkService.read(path + "/" + partition)
      val tp = TopicAndPartition(topic.head, partition)
      val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
      val consumerMin = new SimpleConsumer(leader, port, 10000, 10000, "getMinOffset")
      val currentOffset = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
      var nextOffset = null.asInstanceOf[Long]
      if (StringUtils.isNotEmpty(partitionOffset)) {
        nextOffset = partitionOffset.toLong
      }
      //修改offset越界
      if (currentOffset.length > 0 && nextOffset < currentOffset.head) {
        nextOffset = currentOffset.head
      }
      logger.info("next offset is :" + nextOffset)
      (tp, nextOffset)
    })

  }

  private def getOffsetRanges(kStream: InputDStream[(String, String)]): AtomicReference[Array[OffsetRange]] = {
    kStream.foreachRDD(rdd => {
      val offsetRangeArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.set(offsetRangeArray)
    })
    offsetRanges
  }

  private def updateOffset(kStream: InputDStream[(String, String)], zKPath: String): Boolean = {
    val offsetRanges = getOffsetRanges(kStream)
    val zkClient = zkService.client
    kStream.foreachRDD(rdd => {
      for (o <- offsetRanges.get()) {
        val path = s"${zKPath}/${o.partition}"
        kafka.utils.ZkUtils.updatePersistentPath(zkClient, path, o.fromOffset.toString)
        logger.info(s" update topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} ")
      }
    })
    true
  }

  def getStreamingContext(ssc: StreamingContext): StreamingContext = {



    val kafkaParam = scala.collection.mutable.Map("metadata.broker.list" -> "")
    val topics = ConfigManager.conf.getStringList("kafka.topic").asScala.toList.toSet
    val zKGroupTopicDirs = new ZKGroupTopicDirs("", topics.head)
    val zKPath = s"${zKGroupTopicDirs.consumerOffsetDir}"
    val newOffset = getLastOffset("", topics, zKPath, "", 111)

    //if("smallest".equals(""))
    //{
      kafkaParam += "auto.offset.reset" -> ""
      val kStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParam)
      )
//    //}
//    else{
//      //KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam.toMap, newOffset, messageHandler)
//    }
    logger.info("zKPath is " + zKPath)
    //    val flag = updateOffset(kStream, zKPath)
    //    logger.info("update offset flag is " + flag)
    //    kStream.print()
    val ds = kStream.map(record => (record.key(),record.value()))



    ssc
  }
}
