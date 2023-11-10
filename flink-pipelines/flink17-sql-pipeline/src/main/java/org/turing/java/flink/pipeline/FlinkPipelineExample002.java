package org.turing.java.flink.pipeline;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.turing.java.flink.bean.CarLog;
import org.turing.java.flink.common.utils.FlinkEnvUtils;
import org.turing.java.flink.pipeline.function.CarOverspeedFilter;
import org.turing.java.flink.pipeline.sink.CarOverspeedSink;
import org.turing.java.flink.pipeline.sourcce.CarSpeedSource1ps;

import java.io.InputStream;


/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2023/11/10 15:09
 */
public class FlinkPipelineExample002 {

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

        CarSpeedSource1ps carSpeedSource1ps = new CarSpeedSource1ps();
        DataStreamSource<CarLog> darLogSource = flinkEnv.env().addSource(carSpeedSource1ps);
//        darLogSource.print();

        // 车速超速检测
        SingleOutputStreamOperator<CarLog> overspeedLog = darLogSource
                .filter(new CarOverspeedFilter());

        overspeedLog.addSink(new CarOverspeedSink());

        flinkEnv.env().execute("FlinkPipelineExample002");

    }
}
