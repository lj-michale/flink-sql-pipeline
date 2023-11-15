package org.data.generate.common;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-07-05
 */
public class GenerateUtils {

    private static int randNum;

    private static DateTimeFormatter dataTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * @descri 生成订单Id
     *
     */
    public static long generateRandomOrderId() {
        CountDownLatch countDownLatch = new CountDownLatch(10000000);
        final SnowFlake idWorker = new SnowFlake(0, 0);
        return idWorker.nextId();
    }

    /**
     * @descri 生成用户Id
     *
     */
    public static long generateRandomUserId() {
        long[] userIdArr = {1, 2, 3, 4, 5};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(userIdArr.length - 1);
        return userIdArr[randNum];
    }
    /**
     * @descri 生成用户名称
     */
    public static String generateRandomUserName() {
        String[] userIdArr = {"张珊", "李华", "王武", "陆琪", "张二哈"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(userIdArr.length - 1);
        return userIdArr[randNum];
    }

    /**
     * 获取随机区间小数
     *
     * @param min
     * @param max
     * @return
     */
    public static BigDecimal getRandomRedPacketBetweenMinAndMax(BigDecimal min,
                                                                BigDecimal max) {
        float minF = min.floatValue();
        float maxF = max.floatValue();
        // 生成随机数
        BigDecimal db = new BigDecimal(Math.random() * (maxF - minF) + minF);

        //返回保留两位小数的随机数。不进行四舍五入
        return db.setScale(2,BigDecimal.ROUND_DOWN);
    }

    /**
     * 获取当前系统时间
     *
     */
    public static String getCurrentTime() {
        long currentTime = SystemClock.now();
        String strCurrentTime = sdf.format(currentTime);

        return strCurrentTime;
    }

    /**
     * 获取付款方式
     *
     */
    public static String getPayType() {
        String[] payTypeArr = {"支付宝", "微信", "QQ", "云闪付", "现金", "网银"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(payTypeArr.length - 1);

        return payTypeArr[randNum];
    }

    /**
     * 获取用户来源
     *
     */
    public static String getSourceType() {
        String[] sourceTypeArr = {"PC商城", "小程序", "微信公众号", "Android-APP", "IOS-APP"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(sourceTypeArr.length - 1);

        return sourceTypeArr[randNum];
    }

    /**
     * 获取订单类型
     *
     */
    public static String getOrderType() {
        String[] orderTypeArr = {"PC商城", "小程序", "微信公众号", "Android-APP", "IOS-APP"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(orderTypeArr.length - 1);

        return orderTypeArr[randNum];
    }

    /**
     * 获取物流公司名称
     *
     */
    public static String getDeliveryCompany() {
        String[] deliveryCompanyArr = {"京东物流", "圆通物流", "达达物流", "顺丰物流", "申通物流", "韵达物流", "邮政物流"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(deliveryCompanyArr.length - 1);

        return deliveryCompanyArr[randNum];
    }

    /**
     * 获取收货省份
     *
     */
    public static String getReceiverProvince() {
        String[] receiverProvinceArr = {"北京", "河北", "天津", "上海", "江苏", "湖南", "黑龙江", "南通", "常德", "衡阳",
                "广州", "深圳", "阳江", "茂名", "内蒙古", "安徽", "大连", "扬州", "南京", "佛山", "厦门", "海南"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(receiverProvinceArr.length - 1);

        return receiverProvinceArr[randNum];
    }


    /**
     * 获取订单类型
     *
     */
    public static String getOrderStatu() {
        String[] orderStatuArr = {"已下单", "已出货", "在途", "已送达", "退货"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(orderStatuArr.length - 1);

        return orderStatuArr[randNum];
    }


    /**
     * 获取
     *
     */
    public static String getApkVersion() {
        String[] apkVerionArr = {"2.0.1", "2.0.2", "2.1.3", "2.2.9", "2.1.0", "3.1.1"};
        Random nameRandom = new Random();
        randNum = nameRandom.nextInt(apkVerionArr.length - 1);

        return apkVerionArr[randNum];
    }


    public static void main(String[] args) {

        System.out.println(getCurrentTime());

    }



}