package org.data.generate.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.data.generate.common.GenerateUtils.*;


/**
 * @descri TuringKafkaProducer
 *
 * @author lj.michale
 * @date 2022-06-25
 */
public class TuringKafkaProducer extends Thread {

    private static final Logger log = LoggerFactory.getLogger(TuringKafkaProducer.class);

    private String topic;

    private static final BigDecimal minNum = new BigDecimal(0.01);

    private static final BigDecimal maxNum = new BigDecimal(20000.00);

    private static final BigDecimal coefficient = new BigDecimal(0.05);

    private static final BigDecimal coefficient2 = new BigDecimal(0.00);

    public TuringKafkaProducer(String topic) {
        super();
        this.topic = topic;
    }

    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh-master1:9092,cdh-master2:9092,cdh-master3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 10);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("message.timeout.ms", "3000");

        return new KafkaProducer<String, String>(props);
    }

    @Override
    public void run() {
        Producer<String, String> producer = createProducer();
        List<String> msgList = new ArrayList<String>();

        while (true) {

            try {
                long time = System.currentTimeMillis();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String currentTime = dateFormat.format(time);

                String[] recAccountName = {"红杉资本", "国泰君安", "龙珠资本", "中金支付", "中国银联", "腾讯战投"};
                String[] paltform = {"google", "apple", "mi", "huwei", "vivo", "oppo"};
                String[] channel = {"android", "ios", "wins", "lunix"};

                Random nameRandom = new Random();
                int randomNameNum = nameRandom.nextInt(recAccountName.length - 1);
                int randomNameNum2 = nameRandom.nextInt(paltform.length - 1);
                int randomNameNum3 = nameRandom.nextInt(channel.length - 1);

                String msg001 = "{\n" +
                        "    \"msg_name\":\"msg-001\",\n" +
                        "    \"data\":{\n" +
                        "        \"name\":\"小名\",\n" +
                        "        \"age\":19,\n" +
                        "        \"subTradeList\":[\n" +
                        "            {\n" +
                        "                \"balance\":\"2083.63\",\n" +
                        "                \"money\":\"0.01\",\n" +
                        "                \"tradeDesc\":\"工资\",\n" +
                        "                \"tradeTime\":\"" + currentTime + "\",\n" +
                        "                \"tradeType\":\"1\",\n" +
                        "                \"queryTime\":\"2018-12-07 15:33:07\",\n" +
                        "                \"recAccount\":\"410350248160111\",\n" +
                        "                \"recAccountName\":\"" + recAccountName[randomNameNum] + "\",\n" +
                        "                 \"tradeAddress\":\"上海\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"balance\":\"2083.63\",\n" +
                        "                \"money\":\"0.01\",\n" +
                        "                \"tradeDesc\":\"代付\",\n" +
                        "                \"tradeTime\": \""+ currentTime + "\",\n" +
                        "                \"tradeType\":\"1\",\n" +
                        "                \"queryTime\":\"2018-12-07 15:33:07\",\n" +
                        "                \"recAccount\":\"410350248160111\",\n" +
                        "                \"recAccountName\":\"" + recAccountName[randomNameNum] + "\",\n" +
                        "                \"tradeAddress\":\"上海\"\n" +
                        "            }\n" +
                        "        ]\n" +
                        "    }\n" +
                        "}";

                String msg002 = "{\n" +
                        "  \"msg_name\": \"msg-002\",\n" +
                        "  \"data\": \"10501|32001|396|108820396|554|396|" + time + "|1514223020|34200|0|2|-400|0|1000|1|0|0|866|0|0|0|8|0|0|null|0||396|0|0|\"\n" +
                        "}\n";

                String msg003 = "{\n" +
                        "    \"msg_name\":\"msg-003\",\n" +
                        "    \"data\":{\n" +
                        "        \"msg_id\":\"58546795155875852\",\n" +
                        "        \"tmsg_id\":\"sdm57896617083100511os\",\n" +
                        "        \"result\":\"" + randomNameNum + "\",\n" +
                        "        \"sdk_type\":\"1\",\n" +
                        "        \"itime\":\"" + currentTime + "\",\n" +
                        "        \"type\":\"third_msg_status\",\n" +
                        "        \"account_id\":\"\",\n" +
                        "        \"core_sdk_ver\":\"2.7.1\",\n" +
                        "        \"channel\":\"" + channel[randomNameNum3] + "\",\n" +
                        "        \"app_versio\":\"1.0\",\n" +
                        "        \"platform\":\"" + paltform[randomNameNum2] + "\",\n" +
                        "        \"sdk_ver\":\"4.0.5\",\n" +
                        "        \"app_key\":\"35faa0b054a629f4d75d6046\",\n" +
                        "        \"uid\":\"8024171219\",\n" +
                        "        \"http_report_ver\":\"v3\",\n" +
                        "        \"remote_ip\":\"183.3.220.130\",\n" +
                        "        \"rtime\":\"1617083128\"\n" +
                        "    }\n" +
                        "}";

                String msg004 = "{\n" +
                        "    \"msg_name\":\"msg-004\",\n" +
                        "    \"data\":{\n" +
                        "        \"uid\":\"51348453685\",\n" +
                        "        \"password\":\"3981047001977454592\",\n" +
                        "        \"imei\":\"867614040939506\",\n" +
                        "        \"imsi\":\"460028638213237\",\n" +
                        "        \"pkgname\":\"com.xmbranch.waterstep\",\n" +
                        "        \"appkey\":\"506da2a292ec627146785b13\",\n" +
                        "        \"rtime\":\"" + currentTime + "\",\n" +
                        "        \"apkver\":\"2.0.9\",\n" +
                        "        \"platform\":\"" + randomNameNum + "\",\n" +
                        "        \"business\":\"" + randomNameNum + "\",\n" +
                        "        \"accountid\":\"\",\n" +
                        "        \"sysver\":\"8.1.0,27\",\n" +
                        "        \"model\":\"vivo Y85A\",\n" +
                        "        \"basever\":\"953_GEN_PACK-1.178380.1.179545.1\",\n" +
                        "        \"build\":\"PD1730\",\n" +
                        "        \"channel\":\"developer-default\",\n" +
                        "        \"sdkver\":\"4.0.6\",\n" +
                        "        \"resolution\":\"1080*2154\",\n" +
                        "        \"install\":0,\n" +
                        "        \"sdkstatver\":\"\",\n" +
                        "        \"sdksharever\":\"\",\n" +
                        "        \"sdkcorever\":\"2.6.0\",\n" +
                        "        \"verification_sdk_ver\":\"\",\n" +
                        "        \"device_id_status\":0,\n" +
                        "        \"device_id\":\"d1b88144b6d3c6a0193564e099f767b5\",\n" +
                        "        \"new_imei\":\"867614040939506\",\n" +
                        "        \"android_id\":\"7ec84480e47081d5\",\n" +
                        "        \"mac_address\":\"28:31:66:e6:c6:c9\",\n" +
                        "        \"serial\":\"4c36a98c\",\n" +
                        "        \"udid\":\"\",\n" +
                        "        \"oaid\":\"\",\n" +
                        "        \"vaid\":\"\",\n" +
                        "        \"aaid\":\"\",\n" +
                        "        \"gadid\":\"\",\n" +
                        "        \"android_ver\":\"27\",\n" +
                        "        \"android_target_ver\":\"26\",\n" +
                        "        \"client_ip\":\"113.121.48.97\"\n" +
                        "    }\n" +
                        "}";

                // 模拟电商交易实时数据
                String oms_order = "{\n" +
                        "    \"msg_name\":\"oms_order\",\n" +
                        "    \"data\":{\n" +
                        "        \"id\":" + generateRandomOrderId() + ",\n" +
                        "        \"user_id\":" + generateRandomUserId() + ",\n" +
                        "        \"coupon_id\":2,\n" +
                        "        \"order_sn\":\"201809150101000001\",\n" +
                        "        \"create_time\":\"" + getCurrentTime() + "\",\n" +
                        "        \"user_name\":\"" + generateRandomUserName() + "\",\n" +
                        "        \"total_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum) + "\",\n" +
                        "        \"pay_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum) + "\",\n" +
                        "        \"freight_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum).multiply(coefficient).setScale(2,BigDecimal.ROUND_DOWN) + "\",\n" +
                        "        \"promotion_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum).multiply(coefficient2).setScale(2,BigDecimal.ROUND_DOWN) + "\",\n" +
                        "        \"integration_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum).multiply(coefficient2).setScale(2,BigDecimal.ROUND_DOWN) + "\",\n" +
                        "        \"coupon_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum).multiply(coefficient2).setScale(2,BigDecimal.ROUND_DOWN) + "\",\n" +
                        "        \"discount_amount\":\"" + getRandomRedPacketBetweenMinAndMax(minNum, maxNum).multiply(coefficient2).setScale(2,BigDecimal.ROUND_DOWN) + "\",\n" +
                        "        \"pay_type\":\"" + getPayType() + "\",\n" +
                        "        \"source_type\":\"" + getSourceType() + "\",\n" +
                        "        \"status\":\"" + getOrderStatu() + "\",\n" +
                        "        \"order_type\":\"" + getSourceType() + "\",\n" +
                        "        \"delivery_company\":\"" + getDeliveryCompany() + "\",\n" +
                        "        \"delivery_sn\":\"\",\n" +
                        "        \"auto_confirm_day\":15,\n" +
                        "        \"integration\":13284,\n" +
                        "        \"growth\":13284,\n" +
                        "        \"promotion_info\":\"单品促销,打折优惠：满3件，打7.50折,满减优惠：满1000.00元，减120.00元,满减优惠：满1000.00元，减120.00元,无优惠\",\n" +
                        "        \"bill_type\":null,\n" +
                        "        \"bill_header\":null,\n" +
                        "        \"bill_content\":null,\n" +
                        "        \"bill_receiver_phone\":null,\n" +
                        "        \"bill_receiver_email\":null,\n" +
                        "        \"receiver_name\":\"大梨\",\n" +
                        "        \"receiver_phone\":\"18033441849\",\n" +
                        "        \"receiver_post_code\":\"518000\",\n" +
                        "        \"receiver_province\":\"" + getReceiverProvince() + "\",\n" +
                        "        \"receiver_city\":\"XXX市\",\n" +
                        "        \"receiver_region\":\"XXX区\",\n" +
                        "        \"receiver_detail_address\":\"XXX街道\",\n" +
                        "        \"note\":\"111\",\n" +
                        "        \"confirm_status\":0,\n" +
                        "        \"delete_status\":0,\n" +
                        "        \"use_integration\":null,\n" +
                        "        \"payment_time\":null,\n" +
                        "        \"delivery_time\":null,\n" +
                        "        \"receive_time\":null,\n" +
                        "        \"comment_time\":null,\n" +
                        "        \"modify_time\":1573318228000\n" +
                        "    }\n" +
                        "}";

                msgList.add(msg001);
                msgList.add(msg002);
                msgList.add(msg003);
                msgList.add(msg004);
                msgList.add(oms_order);

                Random random = new Random();
                int i = random.nextInt(msgList.size());

                log.info("随机获取第:{}条消息, 模拟Kafka发送数据: {}", i, msgList.get(i));
                producer.send(new ProducerRecord<String, String>(this.topic, msgList.get(i)));
                producer.flush();

                int randomValue = getRandom(500, 1000);
                log.info("当前随机值:{}", randomValue);

                sleep(randomValue);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("模拟发送Kafka消息异常:{}", e.getMessage());
            }
        }

    }

    public static int getRandom(int min, int max){
        Random random = new Random();
        int s = random.nextInt(max) % (max - min + 1) + min;

        return s;
    }

    public static void main(String[] args) {
        new TuringKafkaProducer("game-events").run();
    }


}