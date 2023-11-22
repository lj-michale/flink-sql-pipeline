package org.turing.flink.pipeline;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.turing.flink.common.utils.FlinkEnvUtils;
import org.turing.flink.pipeline.function.CountWindowAverage;

import java.io.InputStream;

/**
 * @descri: FlinkPipelineExample001
 *
 * @author: lj.michale
 * @date: 2023/11/22 16:07
 */
public class FlinkPipelineExample001 {
    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample001.class);
    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = FlinkPipelineExample001.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info(" flink.pipeline.parallelism:{} ", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getBatchTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        flinkEnv.env().fromElements(
                Tuple2.of(1L, 3L),
                        Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(1L, 4L),
                        Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();

        logger.info(" 作业执行结束! ");

        flinkEnv.env().execute("FlinkPipelineExample001");

    }

}
