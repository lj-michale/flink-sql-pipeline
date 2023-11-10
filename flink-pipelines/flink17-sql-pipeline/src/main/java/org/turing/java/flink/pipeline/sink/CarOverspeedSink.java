package org.turing.java.flink.pipeline.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.turing.java.flink.bean.CarLog;
import java.util.Date;
import java.text.SimpleDateFormat;
/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2023/11/10 15:27
 */
public class CarOverspeedSink implements SinkFunction<CarLog> {
    @Override
    public void invoke(CarLog value, Context context) throws Exception {
        System.out.println(value.getCarCode() + "于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new Date(value.getLogTime())) + " 超速。速度:" + value.getSpeed() + "km/h");
    }

}
