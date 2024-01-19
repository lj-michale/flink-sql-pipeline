package org.turing.bigdata.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, get_json_object, lit, when}
import org.apache.spark.sql.types.{DecimalType, LongType}

/**
 * @descri: HudiDwm2DwsJob
 * spark-submit --class Dwm2DwsJob \
             --master yarn \
             --deploy-mode cluster \
             --name HudiDwm2DwsJob \
             --conf spark.driver.memory=2g \
             --conf spark.driver.cores=2 \
             --conf spark.executor.instances=4 \
             --conf spark.executor.memory=8g \
             --conf spark.executor.cores=2 \
             --conf spark.yarn.submit.waitAppCompletion=false \
             --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
             --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
             --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
             ./hudi-spark-streaming-example.jar
 *
 * @author: lj.michale
 * @date: 2024/1/19 10:52
 */
object Dwm2DwsJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    val sourceLocation ="/xxx/hudi/order_dw.db/dwm_shop_users"
    val targetLocation1 = "/xxx/hudi/order_dw.db/dws_users"
    val checkpointDir1 = "/xxx/hudi/order_dw.db/dws_users_checkpoint"
    val targetLocation2 = "/xxx/hudi/order_dw.db/dws_shops"
    val checkpointDir2 = "/xxx/hudi/order_dw.db/dws_shops_checkpoint"

    import spark.implicits._

    val df = spark.readStream
      .format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.query.incremental.format", "cdc")
      .load(sourceLocation)

    df.select(
        get_json_object($"after", "$.user_id").cast(LongType).alias("user_id"),
        get_json_object($"after", "$.ds").alias("ds"),
        when(get_json_object($"before", "$.fee_sum").isNotNull, get_json_object($"after", "$.fee_sum").cast(DecimalType(20, 2)) - get_json_object($"before", "$.fee_sum").cast(DecimalType(20, 2)))
          .otherwise(get_json_object($"after", "$.fee_sum").cast(DecimalType(20, 2)))
          .alias("fee_sum"))
      .writeStream
      .format("hudi")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "user_id, ds")
      .option("hoodie.datasource.write.precombine.field", "ds")
      .option("hoodie.database.name", "order_dw")
      .option("hoodie.table.name", "dws_users")
      .option("hoodie.metadata.enable", "false")
      .option("hoodie.index.type", "BUCKET")
      .option("hoodie.bucket.index.num.buckets", "8")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.record.merger.impls", "org.apache.hudi.common.model.merger.OrdersLakeHouseMerger")
      .option("hoodie.parquet.compression.codec", "snappy")
      .option("checkpointLocation", checkpointDir1)
      .start(targetLocation1)

    df.select(
        get_json_object($"after", "$.shop_id").cast(LongType).alias("shop_id"),
        get_json_object($"after", "$.ds").alias("ds"),
        when(get_json_object($"before", "$.fee_sum").isNotNull, lit(0L)).otherwise(lit(1L)).alias("uv"),
        when(get_json_object($"before", "$.fee_sum").isNotNull, get_json_object($"after", "$.pv").cast(LongType) - get_json_object($"before", "$.pv").cast(LongType))
          .otherwise(get_json_object($"after", "$.pv").cast(LongType))
          .alias("pv"),
        when(get_json_object($"before", "$.fee_sum").isNotNull, get_json_object($"after", "$.fee_sum").cast(DecimalType(20, 2)) - get_json_object($"before", "$.fee_sum").cast(DecimalType(20, 2)))
          .otherwise(get_json_object($"after", "$.fee_sum").cast(DecimalType(20, 2)))
          .alias("fee_sum"))
      .writeStream
      .format("hudi")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "shop_id, ds")
      .option("hoodie.datasource.write.precombine.field", "ds")
      .option("hoodie.database.name", "order_dw")
      .option("hoodie.table.name", "dws_shops")
      .option("hoodie.metadata.enable", "false")
      .option("hoodie.index.type", "BUCKET")
      .option("hoodie.bucket.index.num.buckets", "8")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.record.merger.impls", "org.apache.hudi.common.model.merger.OrdersLakeHouseMerger")
      .option("hoodie.parquet.compression.codec", "snappy")
      .option("checkpointLocation", checkpointDir2)
      .start(targetLocation2)

    spark.streams.awaitAnyTermination()
  }
}