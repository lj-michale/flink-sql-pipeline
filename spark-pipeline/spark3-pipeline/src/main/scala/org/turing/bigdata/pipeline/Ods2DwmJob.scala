package org.turing.bigdata.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, lit}

/**
 * @descri: Hudi Spark Streaming 作业示例代码
 * spark-submit --class Ods2DwmJob \
             --master yarn \
             --deploy-mode cluster \
             --name HudiOds2DwmJob \
             --conf spark.driver.memory=2g \
             --conf spark.driver.cores=2 \
             --conf spark.executor.instances=4 \
             --conf spark.executor.memory=16g \
             --conf spark.executor.cores=2 \
             --conf spark.yarn.submit.waitAppCompletion=false \
             --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
             --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
             --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
             ./hudi-spark-streaming-example.jar
 *
 * @author: lj.michale
 * @date: 2024/1/19 10:48
 */
object Ods2DwmJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    val sourceLocation ="/xxx/hudi/order_dw.db/ods_orders"
    val targetLocation = "/xxx/hudi/order_dw.db/dwm_shop_users"
    val checkpointDir = "/xxx/hudi/order_dw.db/dwm_shop_users_checkpoint"

    import spark.implicits._

    spark.readStream
      .format("hudi")
      .load(sourceLocation)
      .select(
        $"order_shop_id".alias("shop_id"),
        $"order_user_id".alias("user_id"),
        date_format($"order_create_time", "yyyyMMddHH").alias("ds"),
        lit(1L).alias("pv"),
        $"order_fee".alias("fee_sum")
      )
      .writeStream
      .format("hudi")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "shop_id, user_id, ds")
      .option("hoodie.datasource.write.precombine.field", "ds")
      .option("hoodie.database.name", "order_dw")
      .option("hoodie.table.name", "dwm_shop_users")
      .option("hoodie.metadata.enable", "false")
      .option("hoodie.index.type", "BUCKET")
      .option("hoodie.bucket.index.num.buckets", "8")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.record.merger.impls", "org.apache.hudi.common.model.merger.OrdersLakeHouseMerger")
      .option("hoodie.parquet.compression.codec", "snappy")
      .option("hoodie.table.cdc.enabled", "true")
      .option("hoodie.table.cdc.supplemental.logging.mode", "data_before_after")
      .option("checkpointLocation", checkpointDir)
      .start(targetLocation)

    spark.streams.awaitAnyTermination()
  }

}