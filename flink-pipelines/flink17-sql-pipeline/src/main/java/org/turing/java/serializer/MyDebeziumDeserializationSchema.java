package org.turing.java.serializer;

import com.alibaba.fastjson2.JSON;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.turing.java.pipeline.FlinkPipelineExample001;

/**
 * @descri: Mysql CDC 自定义序列化类
 *
 * @author: lj.michale
 * @date: 2023/11/10 17:42
 */
public class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample001.class);

    public static final String CREATE = "c";
    public static final String DELETE = "d";
    public static final String UPDATE = "u";
    public static final String READ = "r";

    @Override
    public void deserialize(SourceRecord sourceRecord,
                            Collector collector) throws Exception {
        Struct value = (Struct) sourceRecord.value();
        String op = value.getString("op");
        Struct data = null;

        if (CREATE.equals(op)) {
            //增加
            data = this.createData(value);
        } else if (DELETE.equals(op)) {
            //删除
            data = this.deleteData(value);
        } else if (UPDATE.equals(op)) {
            //修改
            data = this.updateData(value);
        } else if (READ.equals(op)) {
            //读取数据
        } else {
            throw new RuntimeException("data is error......");
        }

        collector.collect(JSON.toJSONString(data));
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    private Struct updateData(Struct value) {
        logger.info(" 数据|修改操作");

        Struct beforeData = (Struct) value.get("before");
        Struct afterData = (Struct) value.get("after");
        logger.info("修改之前的数据:{} ", beforeData.toString());
        logger.info("修改之后的数据:{} ", afterData.toString());

        return afterData;
    }

    private Struct deleteData(Struct value) {
        logger.info("数据|删除");
        Struct beforeData = (Struct) value.get("before");
        logger.info("删除数据:{} ", beforeData.toString());

        return beforeData;
    }

    private Struct createData(Struct value) {
        logger.info("数据|增加");
        Struct afterData = (Struct) value.get("after");
        logger.info("增加数据:{} ", afterData.toString());

        return afterData;
    }

}