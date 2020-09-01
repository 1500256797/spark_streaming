package monster.zzqtrojan.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
黑名单过滤实战
重要程度：5新
 */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("TransformApp")

    // 创建streamingContext需要两个参数
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //构建黑名单
    val blacks = List("zs","ls")
    //集合转成dstream
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))
    val lines  = ssc.socketTextStream("hadoop000",6789)

    //日志转成tuple返回的是Dstream
    //t._1 访问第一个元素， t._2 访问第二个元素
    val clicklog = lines.map(x=>(x.split(",")(1),x)).transform(rdd=>{
      rdd.leftOuterJoin(blacksRDD)
        .filter(x=>x._2._2.getOrElse(false)!=true)
        .map(x=>x._2._1)
    })
    //Dstream如何跟blackRDD做操作？ ===>用transform
    clicklog.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
