//package org.turing.bigdata.pipeline
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{date_format, lit}
//
//
///**
// * @descri: Paimon Spark Streaming 作业示例代码
//  spark-submit --class Ods2DwmJob \
//               --master yarn \
//               --deploy-mode cluster \
//               --name PaimonOds2DwmJob \
//               --conf spark.driver.memory=2g \
//               --conf spark.driver.cores=2 \
//               --conf spark.executor.instances=4 \
//               --conf spark.executor.memory=16g \
//               --conf spark.executor.cores=2 \
//               --conf spark.yarn.submit.waitAppCompletion=false \
//               ./paimon-spark-streaming-example.jar
// * @author: lj.michale
// * @date: 2024/1/19 10:47
// */
//object PaimonOds2DwmJob {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().getOrCreate()
//    val sourceLocation = "/xxx/paimon/order_dw.db/ods_orders"
//    val targetLocation = "/xxx/paimon/order_dw.db/dwm_shop_users"
//    val checkpointDir = "/xxx/paimon/order_dw.db/dwm_shop_users_checkpoint"
//    import spark.implicits._
//
//    spark.readStream
//      .format("paimon")
//      .load(sourceLocation)
//      .select(
//        $"order_shop_id",
//        $"order_user_id",
//        date_format($"order_create_time", "yyyyMMddHH").alias("ds"),
//        lit(1L),
//        $"order_fee"
//      )
//      .writeStream
//      .format("paimon")
//      .option("checkpointLocation", checkpointDir)
//      .start(targetLocation)
//
//    spark.streams.awaitAnyTermination()
//  }
//}