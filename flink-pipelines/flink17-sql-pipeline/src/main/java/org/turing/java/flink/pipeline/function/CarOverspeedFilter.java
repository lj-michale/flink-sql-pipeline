package org.turing.java.flink.pipeline.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.turing.common.AbilityDescri;
import org.turing.java.flink.bean.CarLog;

/**
 * @descri: 车辆超速过滤
 *
 * @author: lj.michale
 * @date: 2023/11/10 15:25
 */
@AbilityDescri(desc = "车辆超速过滤")
public class CarOverspeedFilter implements FilterFunction<CarLog> {

    @AbilityDescri(desc = "过滤算子")
    @Override
    public boolean filter(CarLog carLog) throws Exception {
        return carLog.getSpeed() > 120 ? true : false;
    }

}
