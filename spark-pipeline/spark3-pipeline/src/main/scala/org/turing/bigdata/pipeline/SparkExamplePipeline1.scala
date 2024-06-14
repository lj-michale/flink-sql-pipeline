package org.turing.bigdata.pipeline

import org.apache.spark.sql.SparkSession

/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2024/1/19 15:06
 */
object SparkExamplePipeline1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.port.maxRetries",10000)
      .appName("spark-demo")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    spark.sql("show databases").show()
    spark.sql("show tables").show()
    spark.sql("select * from hive_ex3 limit 20").show()
    spark.sql(
      """
        |select grade,
        |       sum(loanamnt) as amount
        |from hive_ex3
        |group by grade
        |""".stripMargin).show()

    println(" >>>>>>>>>>>>>>>>>>>>>> ")

    spark.stop()

  }

}
