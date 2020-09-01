package monster.zzqtrojan.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
Spark streaming 整合flume实战 方式1 ：push
 */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")

    // 创建streamingContext需要两个参数
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //TODO 使用spark streaming 整合flume
    val flumeStream = FlumeUtils.createStream(ssc,"0.0.0.0",41414)

    //获取内容
    val lines = flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
