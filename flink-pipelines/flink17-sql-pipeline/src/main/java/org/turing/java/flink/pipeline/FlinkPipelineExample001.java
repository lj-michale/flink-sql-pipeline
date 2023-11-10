package org.turing.java.flink.pipeline;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.turing.java.flink.common.utils.FlinkEnvUtils;

import java.io.InputStream;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-06-25
 */
public class FlinkPipelineExample001 {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample001.class);

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = FlinkPipelineExample001.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getStreamTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        // 状态后端使用RocksDB
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        String createTableSql = "CREATE TABLE source_table (\n"
                + "    id BIGINT,\n"
                + "    money BIGINT,\n"
                + "    row_time AS cast(CURRENT_TIMESTAMP as timestamp_LTZ(3)),\n"
                + "    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'datagen',\n"
                + "  'rows-per-second' = '1',\n"
                + "  'fields.id.min' = '1',\n"
                + "  'fields.id.max' = '100000',\n"
                + "  'fields.money.min' = '1',\n"
                + "  'fields.money.max' = '1000000'\n"
                + ")\n";

        String querySql = "SELECT UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 as window_end, \n"
                + "      window_start, \n"
                + "      sum(money) as sum_money,\n"
                + "      count(distinct id) as count_distinct_id\n"
                + "FROM TABLE(CUMULATE(\n"
                + "         TABLE source_table\n"
                + "         , DESCRIPTOR(row_time)\n"
                + "         , INTERVAL '5' SECOND\n"
                + "         , INTERVAL '1' DAY))\n"
                + "GROUP BY window_start, \n"
                + "        window_end";

        flinkEnv.streamTEnv().executeSql(createTableSql);
        Table resultTable = flinkEnv.streamTEnv().sqlQuery(querySql);

        flinkEnv.streamTEnv()
                .toDataStream(resultTable, Row.class)
                .flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public void flatMap(Row value, Collector<Object> out) throws Exception {
                        long l = Long.parseLong(String.valueOf(value.getField("sum_money")));

                        if (l > 10000L) {
                            logger.info("报警，超过 1w");
                        }
                    }
                });

        flinkEnv.env().execute("FlinkPipelineExample001");
    }
}
