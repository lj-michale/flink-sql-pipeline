package org.turing.bigdata.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
 * @descri:
 * @author: lj.michale
 * @date: 2024/1/19 16:03
 */
object InitUtil {

  private val logger = Logger.getLogger(this.getClass)

  /**
   * @descri:
   *
   * @param: sparkConf
   * @return:
   */
  def initSparkSession(sparkConf: SparkConf): SparkSession = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://bigdata101:9000")

    sparkSession
  }

}
