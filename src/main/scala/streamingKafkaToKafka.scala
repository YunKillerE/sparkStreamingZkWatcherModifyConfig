
import java.util.Properties

import com.beust.jcommander.JCommander
import common.{KafkaSink}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

class streamingKafkaToKafka {

}


/**
  * 需求：从kafka中消费数据，处理后写回kafka，需要支持动态改变配置，就是在不停止流的情况下改变输出结果
  *
  * 步骤：
  * 1,从kafka消费数据
  * 2,在zk中注册配置，在处理数据时动态获取配置
  * 3,然后将处理结果写回kafka
  *
  * 数据：
  * 1,输入数据格式：11,22,33,44,55
  * 2,输出数据为上面的一个字段
  *
  * 目标：
  * 动态改变配置，然后输出不同列的值
  *
  *
  */


object streamingKafkaToKafka {

  private val log = Logger.getLogger(classOf[streamingKafkaToKafka])

  def main(args: Array[String]): Unit = {

    /**
      * 获取输入参数与定义全局变量
      */

    log.info("获取输入变量")
    val argv = new Args()
    JCommander.newBuilder().addObject(argv).build().parse(args: _*)

    /**
      * 创建source/dest context
      */
    log.info("初始sparkcontext和kuducontext")
    val spark = SparkSession.builder().appName(argv.appName).enableHiveSupport().getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Duration(argv.durationTime))
    ssc.checkpoint("/tmp/streamingToIgnite")

    /**
      * 创建kafka数据流
      */
    log.info("初始化kafka数据流")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> argv.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> argv.groupid,
      "auto.offset.reset" -> "latest",
      "session.timeout.ms" -> "30000",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", argv.brokers)
      p.setProperty("acks", "all")
      p.setProperty("max.in.flight.requests.per.connection", argv.perConnection)
      p.setProperty("batch.size", argv.batchSize)
      p.setProperty("retries", argv.retries)
      p.setProperty("linger.ms", argv.lingerMs)
      p.setProperty("buffer.memory", argv.bufferMem)
      p.setProperty("compression.type", argv.topicCompression)
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }
    val topics = Array(argv.topic)


    val kafkaProducer: Broadcast[KafkaSink[String, Object]] = {
      log.warn("kafka producer init done!")
      spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))


    /**
      * 广播动态配置
      */

    var num: Broadcast[Int] = {
      spark.sparkContext.broadcast(0)
    }

    /**
      * zkwatcher start
      */

    val retryPolicy = new ExponentialBackoffRetry(1000, Integer.MAX_VALUE)
    val curator = CuratorFrameworkFactory.newClient(argv.zkAddress, retryPolicy)

    val path = "/yl"

    curator.start()

    if (curator.checkExists().forPath(path) == null) {
      curator.create().creatingParentsIfNeeded().forPath(path)
    }

    curator.getZookeeperClient.blockUntilConnectedOrTimedOut

    log.error("the original data is " + new String(curator.getData.forPath(path)))

    val nodeCache = new NodeCache(curator, path)

    //启动时获取最新配置
    val cData = nodeCache.getCurrentData
    if(cData.getData != null){
      num = {
        spark.sparkContext.broadcast((new String(cData.getData).toInt))
      }
    }

    nodeCache.getListenable.addListener(new NodeCacheListener() {
      @throws[Exception]
      override def nodeChanged() = {
        val currentData = nodeCache.getCurrentData
        //System.out.println("data change watchecreate()d, and current data = " + new String(currentData.getData))
        log.info("new data ============================= " + (new String(currentData.getData).toInt))

        num = {
          spark.sparkContext.broadcast((new String(currentData.getData).toInt))
        }
      }
    })

    nodeCache.start()

    /**
      * zkwatcher end
      */


    /**
      * 开始处理数据
      */
    log.info("开始处理数据")

    var offsetRanges = Array[OffsetRange]()

    stream.foreachRDD(rdd => {

      // TODO 判断流是否为空，如果为空则不提交任务，节省调度时间
      if (!rdd.isEmpty()) {

        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val valueRDD = rdd.map(_.value().split(","))

        log.info("开始写入kafka")

        valueRDD.foreachPartition(x => {
          x.foreach(x => {

            log.error("num is ============================ " + num.value)
            var tmp = num.value
            if (!(tmp > 0 && tmp < 10)) {
              tmp = 0
            }
            log.error("x is ============================ " + tmp)
            kafkaProducer.value.send("out", x(tmp))
          })
        })

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
