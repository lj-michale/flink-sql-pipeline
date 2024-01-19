package org.turing.java.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * @descri: 通过 Flink DS 跟 SQL 混合模式读取 kafka 数据写入 Paimon 数据湖
 *
 * @author: lj.michale
 * @date: 2024/1/9 10:26
 */
public class FlinkFromKafka2Paimon {

    public static void main(String[] args) {

        //获取流任务的环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE); //打开checkpoint功能
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH)
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.211.106:8020/tmp/flink_checkpoint/FlinkDSFromKafka2Paimon"); //设置checkpoint的hdfs目录
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); //设置checkpoint记录的保留策略

        /**创建flink table环境对象*/
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**添加 Kafka 数据源*/
        KafkaSourceBuilder<String> kafkaSourceBuilder = KafkaSource.<String>builder() //一定注意这里的<String>类型指定，否则后面的new SimpleStringSchema()会报错
                .setBootstrapServers("192.168.211.107:6667")
                .setTopics("test")
                .setGroupId("FlinkFromKafka2Paimon")
                .setStartingOffsets(OffsetsInitializer.earliest()) //确认消费模式为latest
                .setValueOnlyDeserializer(new SimpleStringSchema());



        /**将数据源生成DataStream对象*/
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSourceBuilder.build(), WatermarkStrategy.noWatermarks(), "kafka-data");

        /**将原始DS经过处理后，再添加schema*/
        SingleOutputStreamOperator<Row> targetDS = kafkaDS.map((MapFunction<String, String[]>) line -> {
                    String[] array = line.split("\\|");
                    return array;
                }).filter((FilterFunction<String[]>) array -> {
                    if (array.length == 9) return true;
                    else return false;
                }).map((MapFunction<String[], Row>) array -> Row.ofKind(RowKind.INSERT, array[0], array[1], array[2], array[3], array[4], array[5], array[6], array[7], array[8]))
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"client_ip", "domain", "time", "target_ip", "rcode", "query_type", "authority_record", "add_msg", "dns_ip"},
                                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING)
                );
        /**将目标DS转化成Table对象*/
        Table table = tableEnv.fromDataStream(targetDS);

        /**创建属于paimon类型的catalog*/
        tableEnv.executeSql("CREATE CATALOG hdfs_catalog WITH ('type' = 'paimon', 'warehouse' = 'hdfs://192.168.211.106:8020/tmp/paimon')");

        /**使用创建的catalog*/
        tableEnv.executeSql("USE CATALOG hdfs_catalog");

        /**建paimon表*/
        tableEnv.executeSql("CREATE TABLE if not exists data_from_flink2paimon (`client_ip` STRING ,domain STRING,`time` STRING,target_ip STRING,rcode STRING,query_type STRING,authority_record STRING,add_msg STRING,dns_ip STRING,PRIMARY KEY(client_ip,domain) NOT ENFORCED)");

        /**将 Kafka 数据源登记为当前catalog下的表*/
        tableEnv.createTemporaryView("kafka_table", table);

        /**将Kafka数据写入到 paimon 表中*/
        tableEnv.executeSql("INSERT INTO data_from_flink2paimon SELECT * FROM kafka_table");

    }

}
