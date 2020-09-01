package monster.zzqtrojan.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
Spark streaming 整合kafka 实战：receiver-based
 */
object KafkaReceiverBasedWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverBased")

    if(args.length<4){
      System.err.println("Usage:KafkaReceiverBased <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum,group,topics,numThreads) = args
    // 创建streamingContext需要两个参数
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //TODO 使用spark streaming 整合Kafka
    //TODO 出了创建input streaming 不同，其他业务逻辑都是一样的
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

    //获取内容
    //TODO :自己去测试为啥要取第二个
    messages.map(_._2).flatMap(_.split("")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
