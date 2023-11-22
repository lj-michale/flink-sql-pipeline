package org.turing.java.pipeline.sourcce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.turing.java.bean.CarLog;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @descri: 每秒产生1条数据的车辆速度数据源
 *
 * @author: lj.michale
 * @date: 2023/11/10 15:06
 */
public class CarSpeedSource1ps implements SourceFunction<CarLog> {

    private boolean needRun = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        Random random = new Random();
        CarLog carLog = new CarLog();
        carLog.setCarCode("car_" + random.nextInt(5));

        long logTime = 0;
        int speed = 0;

        while (needRun) {
            logTime = System.currentTimeMillis() - 50 - random.nextInt(500);
            speed=random.nextInt(150);
            carLog.setVin(getVin());
            carLog.setLogTime(logTime);
            carLog.setSpeed(speed);
            carLog.setGpsLongitude(gpsLongitude());
            carLog.setGpsLatitude(gpsLatitude());

            sourceContext.collect(carLog);
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        needRun = false;

    }

    private String getVin() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();
        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return generatedString;
    }

    /**
     * @descri: 生成经度
     *
     * @return:
     */
    private String gpsLongitude() {
        Map<String, String> jwMap = randomLonLat(85, 122, 29, 116);

        return jwMap.get("J");
    }

    /**
     * @descri: 生成纬度
     *
     * @return:
     */
    private String gpsLatitude() {
        Map<String, String> jwMap = randomLonLat(85, 122, 29, 116);

        return jwMap.get("W");
    }

    /**
     * @descri: 在矩形内随机生成经纬度
     *
     * @param minLon：最小经度
     * 		  maxLon： 最大经度
     *  	  minLat：最小纬度
     * 		  maxLat：最大纬度
     * @return @throws
     */
    public static Map<String, String> randomLonLat(double minLon,
                                                   double maxLon,
                                                   double minLat,
                                                   double maxLat) {
        BigDecimal db = new BigDecimal(Math.random() * (maxLon - minLon) + minLon);
        String lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();// 小数后6位
        db = new BigDecimal(Math.random() * (maxLat - minLat) + minLat);
        String lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
        Map<String, String> map = new HashMap<String, String>();
        map.put("J", lon);
        map.put("W", lat);

        return map;
    }

}
