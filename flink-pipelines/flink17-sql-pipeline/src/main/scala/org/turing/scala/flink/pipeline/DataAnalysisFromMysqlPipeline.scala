package org.turing.scala.flink.pipeline

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @descri: DataAnalysisFromMysqlPipeline
 *
 * @author: lj.michale
 * @date: 2023/11/10 17:09
 */
object DataAnalysisFromMysqlPipeline {

  def main(args: Array[String]): Unit = {
    // 定义Table环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    // 表定义了primary key，则以upsert(更新插入)方式插入数据, flink-connector-jdbc_2.12
    val table_str =
      """
        |create temporary table %s(
        |  id int,
        |  name string,
        |  description string,
        |  primary key (id) not enforced
        |) with (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/cdc221-flink114',
        |   'driver' = 'com.mysql.cj.jdbc.Driver',
        |   'table-name' = '%s',
        |   'username' = 'root',
        |   'password' = 'Turing@2022'
        |)
        |""".stripMargin
    // 在catalog注册表
    tEnv.executeSql(table_str.format("products", "products"))
    tEnv.executeSql(table_str.format("products2", "products2"))

    // =====================读取源表数据=====================
    val products = tEnv.from("products") // 方式一
    products.execute().print()
    // val products = tEnv.sqlQuery("select * from products limit 2")   // 方式二

    // =====================向目标表插入数据=====================
    // products.executeInsert("products2")     // 方式一

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("products2", products) // 方式二
    // stmtSet.addInsertSql("insert into user2 select * from products limit 2")   // 方式三

    stmtSet.execute()

  }

}
