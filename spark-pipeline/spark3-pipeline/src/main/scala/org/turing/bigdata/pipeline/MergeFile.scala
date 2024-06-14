//package org.turing.bigdata.pipeline
//
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//
///**
// * @descri: Spark定期合并Hive表小文件Spark代码实现
// *          写个shell脚本传入所需参数，可设定任意的分区开始日期和结束日期，灵活合并Hive表的分区文件
// * @author: lj.michale
// * @date: 2024/1/23 9:46
// */
//object MergeFile {
//
//  def main(args: Array[String]): Unit = {
//
//    val jobName = args(0)   // 任务名
//    val tableName = args(1)  // hive表名
//    val format = args(2).toInt   // 1 text格式 && 2 parquet格式
//    val pa = args(3).toInt // 并发
//    val dt_str = args(4)
//    val dt = args(5)       // 分区天 开始dt
//    val last = args(6)     // 截止dt
//    val hour_str = args(7)
//    val hour = args(8)     // 分区小时
//
//    val spark = SparkSession
//      .builder()
//      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
//      .appName(jobName + "_MergeFile" + dt)
//      .master("yarn")
//      .enableHiveSupport
//      .getOrCreate
//
//    val db = tableName.split("[.]")(0) + ".db"
//    val orgTableName = tableName.split("[.]")(1)
//
//    // 天+小时分区
//    if (!hour_str.equals("null")) {
//      // 原表导入到文件
//      val df: DataFrame = spark.sql(s"select * from ${tableName} where ${dt_str}=${dt} and `${hour_str}`= ${hour} ")
//      val origin_table_path = s"hdfs://emr-cluster/user/hive/warehouse/${db}/${orgTableName}/ ${dt_str}=$dt/hour=$hour"
//
//      if (format == 1) {
//        // text格式文件
//        val text_path = s"hdfs://emr-cluster/user/hive/warehouse/temp.db/${jobName}/${dt_str}=${dt}/${hour_str}=${hour}"
//        df.rdd.map(_.mkString("\001")).coalesce(pa).saveAsTextFile(text_path)
//
//        // 文件导入覆盖原表
//        spark.read.textFile(text_path).write.mode(SaveMode.Overwrite).save(origin_table_path)
//
//      } else {
//        // parquet格式文件
//        val parquet_path = s"hdfs://emr-cluster/user/hive/warehouse/temp.db/${jobName}/${dt_str}=${dt}/${hour_str}=${hour}"
//        df.coalesce(pa).write.mode(SaveMode.Overwrite).parquet(parquet_path)
//
//        // 文件导入覆盖原表
//        spark.read.parquet(parquet_path).write.mode(SaveMode.Overwrite).save(origin_table_path)
//      }
//
//    } else {
//      // 原表导入到文件
//      val df: DataFrame = spark.sql(s"select * from ${tableName} where dt=${dt} ")
//      val origin_table_path = s"hdfs://emr-cluster/user/hive/warehouse/${db}/${orgTableName}/${dt_str}=${dt}"
//
//      if (format == 1) {
//        // text格式文件
//        val text_path = s"hdfs://emr-cluster/user/hive/warehouse/temp.db/${jobName}/${dt_str}=${dt}"
//        df.rdd.map(_.mkString("\001")).coalesce(pa).saveAsTextFile(text_path)
//
//        // 文件导入覆盖原表
//        spark.read.textFile(text_path).write.mode(SaveMode.Overwrite).save(origin_table_path)
//
//      } else {
//        // parquet格式文件
//        val parquet_path = s"hdfs://emr-cluster/user/hive/warehouse/temp.db/${jobName}/${dt_str}=${dt}"
//        df.coalesce(pa).write.mode(SaveMode.Overwrite).parquet(parquet_path)
//
//        val aa = spark.read.parquet(parquet_path)
//        aa.show(10)
//
//        // 文件导入覆盖原表
//        spark.read.parquet(parquet_path).write.mode(SaveMode.Overwrite).save(origin_table_path)
//      }
//    }
//
//    spark.close()
//  }
//
//}
