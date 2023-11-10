package org.turing.java.flink.pipeline;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.turing.java.flink.bean.CarLog;
import org.turing.java.flink.common.utils.FlinkEnvUtils;
import org.turing.java.flink.pipeline.sourcce.CarSpeedSource1ps;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @descri: 汽车超速有状态实时检测|累加超速次数
 *          Java版本不同，反射出错 -> 在Idea中指定如下的VM Options：
 *          --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.math=ALL-UNNAMED
 *
 * @author: lj.michale
 * @date: 2023/11/10 15:09
 */
public class OverspeedDetectionWIthStatePipeline {

    private static final Logger logger = LoggerFactory.getLogger(OverspeedDetectionWIthStatePipeline.class);

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
        DataStreamSource<CarLog> darLogSourceOne = flinkEnv.env().addSource(carSpeedSource1ps);
        DataStreamSource<CarLog> darLogSourceTwo = flinkEnv.env().addSource(carSpeedSource1ps);

        DataStream<CarLog> darLogSource = darLogSourceOne.union(darLogSourceTwo);
        darLogSource.print("darLogSource===============>>>");

        // 车速超速检测
        SingleOutputStreamOperator<Object> darLogDS = darLogSource
                .keyBy(new KeySelector<CarLog, Object>() {
                    @Override
                    public Object getKey(CarLog carLog) throws Exception {
                        return carLog.getCarCode();
                    }
                })
                .flatMap(new RichFlatMapFunction<CarLog, Object>() {
                    ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Integer> overSpeedCount = new ValueStateDescriptor<Integer>("overSpeedCount",
                                TypeInformation.of(new TypeHint<>() {
                                    @Override
                                    public TypeInformation<Integer> getTypeInfo() {
                                        return super.getTypeInfo();
                                    }
                                })
                        );
                        valueState = getRuntimeContext().getState(overSpeedCount);
                    }

                    @Override
                    public void flatMap(CarLog carLog, Collector<Object> collector) throws Exception {
                        Integer value = valueState.value();

                        if (null == value) {
                            value = Integer.valueOf(0);
                        }
                        if (carLog.getSpeed() > 120) {
                            value += 1;
                            valueState.update(value);
                        }

                        collector.collect(Tuple2.of(carLog.getCarCode(), value));
                    }
                });


        darLogDS.print("darLogDS===============>>>");

        flinkEnv.env().execute("OverspeedDetectionWIthStatePipeline");

    }

}
