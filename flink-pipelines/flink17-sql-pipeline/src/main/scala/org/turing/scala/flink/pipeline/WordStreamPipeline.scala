package org.turing.scala.flink.pipeline

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.turing.scala.flink.pipeline.source.WordSource

/**
 * @descri:
 * @author: lj.michale
 * @date: 2023/11/10 17:04
 */
object WordStreamPipeline {

  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance()
      .inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(senv, bsSettings)

    val stream: DataStream[String] = senv.addSource(new WordSource())
    val table = tEnv.fromDataStream(stream).as("word")

    val result = tEnv.sqlQuery("select * from " + table + " where word like '%t%'")
    tEnv.toDataStream(result).print()

    println(senv.getExecutionPlan)
    senv.execute()

  }
}
