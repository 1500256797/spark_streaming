package monster.zzqtrojan.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/*
Spark streaming 整合flume实战 方式1 ：pull
 */
object FlumePullWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePullWordCount")

    // 创建streamingContext需要两个参数
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //TODO 使用spark streaming 整合flume
    //TODO 出了创建input streaming 不同，其他业务逻辑都是一样的
    val flumeStream = FlumeUtils.createPollingStream(ssc,"hadoop000",41414)

    //获取内容
    val lines = flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
