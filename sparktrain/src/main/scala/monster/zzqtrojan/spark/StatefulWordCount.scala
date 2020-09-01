package monster.zzqtrojan.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
计算到目前为止累加出现的单词个数并写入db
场景：统计TOPN
 */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    // 创建streamingContext需要两个参数
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //如果使用的带状态的算子，必须要设置checkpoint
    //在生产环境下，建议把checkPoint设置到hdfs中
    ssc.checkpoint(".")
    val lines  = ssc.socketTextStream("hadoop000",6789)

    //统计状态
    val result = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /*
    把当前的数据去更新已有的或是老的数据
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    // add the new values with the previous running count to get the new count
    Some(current+pre)
  }
}
