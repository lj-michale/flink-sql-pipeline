package org.turing.java.flink.pipeline;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/**
 * @descri:  对长期延迟数据的处理
 *
 * @author: lj.michale
 * @date: 2023/11/9 13:07
 */
@Slf4j
public class DataWithLatenessPipeline {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*******************  source  ****************/
        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.43.102", 9999);

        /*******************  transorform  ****************/
        // 将输入的[单词 时间戳]字符串, 转换为元组
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple = socketTextStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] ele = value.split(" ");
                return Tuple2.of(ele[0], Long.parseLong(ele[1]));
            }
        });

        // 设置事件时间和watermark
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = tuple
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1 * 1000L;
                                    }
                                })
                );

        // 配置窗口, 滚动窗口, 长度5秒
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> windowedStream = watermarks.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        // 增加窗口对长期延迟数据的处理
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> windowedStreamWithLateness = windowedStream
                .allowedLateness(Time.seconds(2));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStreamWithLateness
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String key = null;
                        int count = 0;
                        for (Tuple2<String, Long> ele : input) {
                            count += 1;
                            key = ele.f0;
                        }
                        out.collect(Tuple2.of(key, count));
                        System.out.println("window start: " + window.getStart() + ", " +
                                "window end: " + window.getEnd());
                    }
                });

        /*******************  sink  ****************/
        result.print();

        env.execute();
    }

}
