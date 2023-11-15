package org.data.generate.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.data.generate.common.GenerateUtils.getCurrentTime;

/**
 * @descri: 生成电商交易模拟数据
 *
 * @author: lj.michale
 * @date: 2023/11/15 15:46
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class GenerateEcommerceTransaSimulatData extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(GenerateEcommerceTransaSimulatData.class);

    private String topic;

    //////////////////// 模拟数据 - 字段


    //////////////////// 模拟数据 - 字段

    @Override
    public void run() {
        while (true) {
            try {
                Producer<String, String> kafkaProducer = createProducer();
                List<String> msgList = new ArrayList<String>();
                String startTime = getCurrentTime();

                ///// 测试数据编造逻辑
                String message = " 》》》》》》》》》》》》》》》》 ";


                msgList.add(message);
                String endTime = getCurrentTime();

                Random random = new Random();
                int i = random.nextInt(msgList.size());

                logger.info(" 随机获取第:{}条消息,模拟Kafka发送数据:{}", i, msgList.get(i));
//                producer.send(new ProducerRecord<String, String>(this.topic, msgList.get(i)));
                //key：作用是决定往那个分区上发，value：具体要发送的消息内容
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,"mykeyvalue",msgList.get(i));
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null){
                            System.out.println("发送消息失败");
                        }
                        if (recordMetadata != null){
                            //消息发送的元数据为
                            System.out.println("异步发送消息结果:"+"topic"+recordMetadata.topic()+"partition"+recordMetadata.partition() +"offset"+recordMetadata.offset());
                        }
                    }
                });

                kafkaProducer.flush();
            } catch (Exception e) {
              e.printStackTrace();
              logger.error("模拟发送Kafka消息异常:{}", e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        new GenerateEcommerceTransaSimulatData("kafeidou").run();

    }

    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.43.102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("message.timeout.ms", "3000");

        return new KafkaProducer<String, String>(props);
    }





}
