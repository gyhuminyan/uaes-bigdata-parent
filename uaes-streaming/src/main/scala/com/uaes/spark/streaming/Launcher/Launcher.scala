package com.uaes.spark.streaming.Launcher

import java.util.concurrent.atomic.AtomicReference

import com.uaes.spark.streaming.config.ConfigManager
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by mzhang on 2017/10/16.
  */
object Launcher {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(Launcher.getClass)
    //System.setProperty("hadoop.home.dir", "D:\\winutils\\winutils")
    var scc = null.asInstanceOf[StreamingContext]
    try {
//      val conf = ConfigManager.getConfig()
      val conf = new SparkConf().setAppName("")
      "" match {
        case "local" => conf.setMaster("local[*]")
        case "yarn" => conf.setMaster("yarn-client")
        case _ => {
          logger.error("unknown spark run mode,system exit with code -1,please check your config file")
          System.exit(-1)
        }
      }

      conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      val ssc = new StreamingContext(conf, Seconds(10))
      ssc.checkpoint("E:\\\\Temp\\\\checkpoint")
      //ssc.addStreamingListener(new ConsumerListener(this, this.zkService))

      scc.start()
      scc.awaitTermination()
    } catch {
      case e: Exception => logger.error("failed to create streamingContext:", e)
    }
  }
}
