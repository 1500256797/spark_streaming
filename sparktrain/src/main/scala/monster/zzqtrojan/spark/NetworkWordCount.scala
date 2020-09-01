package monster.zzqtrojan.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
spark streaming 处理socket数据
 */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setMaster("local[2]")
       .setAppName("NetworkWordCount")

      // 创建streamingContext需要两个参数
       val ssc = new StreamingContext(sparkConf,Seconds(5))

       val lines  = ssc.socketTextStream("hadoop000",6789)

       val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

      result.print()

      ssc.start()
      ssc.awaitTermination()

  }
}
