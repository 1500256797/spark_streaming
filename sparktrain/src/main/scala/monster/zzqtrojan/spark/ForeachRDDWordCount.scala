package monster.zzqtrojan.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
计算到目前为止累加出现的单词个数并写入db
场景：统计TOPN
 */
object ForeachRDDWordCount {
  //如果方法没有返回值，可返回unit，类似于java中的void
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    // 创建streamingContext需要两个参数
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //如果使用的带状态的算子，必须要设置checkpoint
    //在生产环境下，建议把checkPoint设置到hdfs中
    ssc.checkpoint(".")
    val lines  = ssc.socketTextStream("hadoop000",6789)

    //统计状态
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //因为又db做支撑，所有不需要累积操作
    //val state = result.updateStateByKey[Int](updateFunction _)

    //控制台
    result.print()
    //TODO 将结果写入到Mysql
//      result.foreachRDD { rdd =>
//      val connection = createConnection()  // executed at the driver
//      rdd.foreach { record =>
//        val sql = "insert into wordcount(word,wordcount) values ('"+record._1+"',"+record._2+")"
//        connection.createStatement().execute(sql)
//      }


    result.foreachRDD(rdd=>{
      //每个rdd的partition创建connection应该换成连接池。
      rdd.foreachPartition(partitionOfRecords=>{
        val connection = createConnection()
        partitionOfRecords.foreach(record=>{
          //sql
          //通常使用Hbase/Redis
          val sql = "insert into wordcount ( word , wordcount ) values ('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        //close
        //关闭connection
        connection.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }



  /*
   获取Mysql的连接
   def functionName ([参数列表]) : [return type]
   方法定义由一个def开始，紧接着括号内是可选的参数列表，一个冒号：和方法的返回值类型
   一个 =，最后是方法的主体。
   */
  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.200.74:3306/basicdata_cs3?useUnicode=true&characterEncoding=utf-8","root","root")
  }
}
