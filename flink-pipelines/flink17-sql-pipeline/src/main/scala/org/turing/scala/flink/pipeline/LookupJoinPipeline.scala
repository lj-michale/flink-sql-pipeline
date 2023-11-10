package org.turing.scala.flink.pipeline

import java.time.ZoneId
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @descri:  LookupJoinPipeline
 *
 * @author: lj.michale
 * @date: 2023/11/10 17:23
 */
object LookupJoinPipeline {

  def main(args: Array[String]): Unit = {

    // 定义Table环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    // 指定时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    val showLogTableSql =
      """-- 曝光日志数据
        |CREATE TABLE show_log (
        |    log_id BIGINT,
        |    `timestamp` as cast(CURRENT_TIMESTAMP as timestamp(3)),
        |    user_id STRING,
        |    proctime AS PROCTIME()
        |)
        |WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '10',
        |  'fields.user_id.length' = '1',
        |  'fields.log_id.min' = '1',
        |  'fields.log_id.max' = '10'
        |);
        |""".stripMargin
    tEnv.executeSql(showLogTableSql)

    val clickLogTableSql =
      """
        |CREATE TABLE user_profile (
        |    user_id STRING,
        |    age STRING,
        |    sex STRING
        |    ) WITH (
        |  'connector' = 'redis',
        |  'hostname' = '127.0.0.1',
        |  'port' = '6379',
        |  'format' = 'json',
        |  'lookup.cache.max-rows' = '500',
        |  'lookup.cache.ttl' = '3600',
        |  'lookup.max-retries' = '1'
        |);
        |""".stripMargin
    tEnv.executeSql(clickLogTableSql)

    // 结果表
    val sinkTableSql =
      """
        |CREATE TABLE sink_table (
        |    log_id BIGINT,
        |    `timestamp` TIMESTAMP(3),
        |    user_id STRING,
        |    proctime TIMESTAMP(3),
        |    age STRING,
        |    sex STRING
        |) WITH (
        |  'connector' = 'print'
        |);
        |""".stripMargin
    tEnv.executeSql(sinkTableSql)

    val joinSQL =
      """
        |-- lookup join 的 query 逻辑
        |INSERT INTO sink_table
        |SELECT
        |    s.log_id as log_id
        |    , s.`timestamp` as `timestamp`
        |    , s.user_id as user_id
        |    , s.proctime as proctime
        |    , u.sex as sex
        |    , u.age as age
        |FROM show_log AS s
        |LEFT JOIN user_profile FOR SYSTEM_TIME AS OF s.proctime AS u
        |ON s.user_id = u.user_id
        |""".stripMargin
    tEnv.executeSql(joinSQL)

  }
}
