package com.adups.test.kafka.demo.producer;

/**
 * Created by allenbai on 2017/2/22 0022.
 */

import com.alibaba.fastjson.JSONObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;


public class KafkaProducerCheck {

    /*
        java -Djava.ext.dirs=/usr/lib/kafka/libs/ -jar BigData.jar kafka.KafkaProducerDemo
     */
    private final Producer<String,String> producer;
    public final static String TOPIC="ota_check";

    private KafkaProducerCheck(){
        Properties props=new Properties();
//        props.put("metadata.broker.list","vm195:9092,vm42:9092,vm61:9092");
        //180.97.69.208:19090,180.97.69.210:19090,180.97.69.211:19090
//        props.put("metadata.broker.list","180.97.69.211:9092,180.97.69.210:9092,180.97.69.208:9092,180.97.69.199:9092");
        props.put("metadata.broker.list","180.97.69.50:19090,180.97.69.210:19090,180.97.69.211:19090");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks","-1");

        producer=new Producer<String,String>(new ProducerConfig(props));
    }

    void produce(){
        int messageNo=1000;
        final int COUNT=10000;

        CheckInfo checkInfo=new CheckInfo();
        checkInfo.setMid("e0aee11a");
        checkInfo.setProductId((long)1500003696);
        checkInfo.setDeltaId((long)14);
//        checkInfo.setLac("43052");
//        checkInfo.setCid("41253");
//        String json=JSONObject.toJSONString(checkInfo);
        Random random=new Random();
        while(messageNo<COUNT){
            String key=String.valueOf(messageNo);
            checkInfo.setLac("lac"+random.nextInt(3));
            checkInfo.setCid("cid"+random.nextInt(2));
            checkInfo.setDeviceId(random.nextInt(6)+"test");
            String json=JSONObject.toJSONString(checkInfo);
            String data =JSONObject.toJSONString(checkInfo);
            producer.send(new KeyedMessage<String, String>(TOPIC,key,data));
            System.out.println(data);
            try {
                Thread.sleep(1000*10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            messageNo++;
        }
    }

    public static void main(String[] args) {
        new KafkaProducerCheck().produce();
    }
}
