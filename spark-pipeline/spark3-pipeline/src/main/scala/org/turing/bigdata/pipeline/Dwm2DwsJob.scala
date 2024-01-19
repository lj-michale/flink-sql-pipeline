package org.turing.bigdata.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}

/**
 * @descri:  Paimon Spark Streaming Dwm2DwsJob
   spark-submit --class Dwm2DwsJob \
             --master yarn \
             --deploy-mode cluster \
             --name PaimonDwm2DwsJob \
             --conf spark.driver.memory=2g \
             --conf spark.driver.cores=2 \
             --conf spark.executor.instances=4 \
             --conf spark.executor.memory=8g \
             --conf spark.executor.cores=2 \
             --conf spark.yarn.submit.waitAppCompletion=false \
             ./paimon-spark-streaming-example.jar
 *
 * @author: lj.michale
 * @date: 2024/1/19 10:51
 */
object Dwm2DwsJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    val sourceLocation = "/xxx/paimon/order_dw.db/dwm_shop_users"
    val targetLocation1 = "/xxx/paimon/order_dw.db/dws_users"
    val checkpointDir1 = "/xxx/paimon/order_dw.db/dws_users_checkpoint"
    val targetLocation2 = "/xxx/paimon/order_dw.db/dws_shops"
    val checkpointDir2 = "/xxx/paimon/order_dw.db/dws_shops_checkpoint"

    import spark.implicits._

    val df = spark.readStream
      .format("paimon")
      .option("read.changelog", "true")
      .load(sourceLocation)

    df.select(
        $"user_id",
        $"ds",
        when($"_row_kind" === "+I" || $"_row_kind" === "+U", $"fee_sum")
          .otherwise($"fee_sum" * -1)
          .alias("fee_sum"))
      .writeStream
      .format("paimon")
      .option("checkpointLocation", checkpointDir1)
      .start(targetLocation1)

    df.select(
      $"shop_id",
      $"ds",
      when($"_row_kind" === "+I" || $"_row_kind" === "+U", lit(1L)).otherwise(lit(-1L)).alias("uv"),
      when($"_row_kind" === "+I" || $"_row_kind" === "+U", $"pv").otherwise($"pv" * -1).alias("pv"),
      when($"_row_kind" === "+I" || $"_row_kind" === "+U", $"fee_sum")
        .otherwise($"fee_sum" * -1)
        .alias("fee_sum")
        .writeStream
        .format("paimon")
        .option("checkpointLocation", checkpointDir2)
        .start(targetLocation2)

        spark.streams.awaitAnyTermination()
  }
}