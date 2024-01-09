package org.turing.java.pipeline;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;


/**
 * @descri: 
 *
 * @author: lj.michale
 * @date: 2024/1/9 10:32
 */
public class StreamingJob {
    private SourceFunction<Long> source;
    private SinkFunction<Long> sink;

    public StreamingJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    public StreamingJob() {
    }

    public void execute() throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()        // access high-level configuration
                .getConfiguration()   // set low-level key-value options
                .setString("table.exec.resource.default-parallelism", String.valueOf(1));//set parallelism value equal to kafka partition of source topic

        tEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `userId` STRING,\n" +
                "  `orderId` STRING,\n" +
                "  `eventTime` TIMESTAMP(3),\n" +
                "  `userName` STRING, \n" +
                "  `priceAmount` DOUBLE ,\n" +
                "  WATERMARK FOR eventTime AS eventTime - INTERVAL '10' SECONDS \n"+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flinktopic',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE SinkTable (\n" +
                "  `userId` STRING,\n" +
                "  `avgPriceAmount` DOUBLE,\n" +
                "  `eventTime` TIMESTAMP,\n" +
                "  `userName` STRING, \n" +
                "  PRIMARY KEY (userId) NOT ENFORCED \n"+
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'flinkout',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'key.format' = 'json' ,\n" +
                "  'value.format' = 'json'\n" +
                ")");


        Table table2 = tEnv.sqlQuery("SELECT userId,AVG(priceAmount) as avgPriceAmount, TUMBLE_START(eventTime, INTERVAL '1' MINUTE) AS eventTime,last_value(userName)  as userName FROM KafkaTable group by TUMBLE( eventTime, INTERVAL '1' MINUTE) ,userId");


// Emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = table2.insertInto("SinkTable").execute();

    }

    public static void main(String[] args) throws Exception {
        StreamingJob job = new StreamingJob();
        job.execute();

    }

}