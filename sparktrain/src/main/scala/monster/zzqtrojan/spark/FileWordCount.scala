package monster.zzqtrojan.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
spark streaming 处理HDFS/本地文件系统的文件数据
 */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    var lines = ssc.textFileStream("C:\\Users\\Administrator\\Desktop\\test")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
