package org.turing.java.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.turing.java.flink.serializer.MyDebeziumDeserializationSchema;

/**
 * @descri: mysql cdc
 *
 * @author: lj.michale
 * @date: 2023/11/10 17:41
 */
public class CtgMemberMysqlCdcPipeline {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启事件时间语义
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        // 状态后端-HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend());
        // 等价于MemoryStateBackend
        env.getCheckpointConfig().setCheckpointStorage("file:///E:\\company\\Turing\\OpenSource\\flink-sql-pipeline\\checkpoint");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("cdc221-flink114")
                .tableList("cdc221-flink114.tbl_user_cdc221-flink14_cus")
                .username("root")
                .password("Turing@2022")
//                .deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(new MyDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-Source")
                .setParallelism(1);
        streamSource.print("最终数据===>");

        env.execute("CtgMemberPipelineMySQLOSourceMonitor");

    }

}
