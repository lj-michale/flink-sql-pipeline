package org.turing.scala.flink.pipeline

import java.time.ZoneId

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @descri: FullIntervalJoinPipeline
 *
 * @author: lj.michale
 * @date: 2023/11/10 17:15
 */
object FullIntervalJoinPipeline {

  def main(args: Array[String]): Unit = {

    // 定义Table环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 曝光日志数据表
    val showLogTableSql =
      """-- 曝光日志数据
        |CREATE TABLE show_log (
        |    log_id BIGINT,
        |    show_params STRING,
        |    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
        |    WATERMARK FOR row_time AS row_time
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.show_params.length' = '1',
        |  'fields.log_id.min' = '5',
        |  'fields.log_id.max' = '15'
        |);
        |""".stripMargin
    tEnv.executeSql(showLogTableSql)

    // -- 点击日志数据表
    val clickLogTableSql =
      """
        |CREATE TABLE click_log (
        |    log_id BIGINT,
        |    click_params STRING,
        |    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
        |    WATERMARK FOR row_time AS row_time
        |)
        |WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.click_params.length' = '1',
        |  'fields.log_id.min' = '1',
        |  'fields.log_id.max' = '10'
        |);
        |""".stripMargin
    tEnv.executeSql(clickLogTableSql)

    // 结果表
    val sinkTableSql =
      """
        |CREATE TABLE sink_table (
        |    s_id BIGINT,
        |    s_params STRING,
        |    c_id BIGINT,
        |    c_params STRING
        |) WITH (
        |  'connector' = 'print'
        |);
        |""".stripMargin
    tEnv.executeSql(sinkTableSql)

    val joinSQL =
      """
        |INSERT INTO sink_table
        |SELECT
        |    show_log.log_id as s_id,
        |    show_log.show_params as s_params,
        |    click_log.log_id as c_id,
        |    click_log.click_params as c_params
        |FROM show_log LEFT JOIN click_log ON show_log.log_id = click_log.log_id
        |AND show_log.row_time BETWEEN click_log.row_time - INTERVAL '5' SECOND AND click_log.row_time + INTERVAL '5' SECOND;
        |""".stripMargin
    tEnv.executeSql(joinSQL)

  }


}
