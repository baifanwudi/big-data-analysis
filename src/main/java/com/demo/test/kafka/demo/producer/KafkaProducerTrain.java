package com.demo.test.kafka.demo.producer;

/**
 * Created by allenbai on 2017/2/22 0022.
 */

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;


public class KafkaProducerTrain {

    private final KafkaProducer<String,String> producer;
    public final static String TOPIC="test-topic";
    //yyyy-MM-dd HH:mm:ss
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat formatSimple = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private KafkaProducerTrain(){
        Properties props=new Properties();
        props.put("bootstrap.servers","kslave1.bigdata.ly:9092,kslave2.bigdata.ly:9092,kslave3.bigdata.ly:9092,kslave4.bigdata.ly:9092");
        props.put("acks","-1");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer=new KafkaProducer<String,String>(props);
    }

    void produce(){
        int messageNo=1000;
        final int COUNT=10000;

        TrainTransferParamDTO trainTransferParamDTO=new TrainTransferParamDTO();
//        trainTransferParamDTO.setDate("20180528");
//        trainTransferParamDTO.getFrom();
        Random random=new Random();
        while(messageNo<COUNT){
            String key=String.valueOf(messageNo);
            long nowTime=System.currentTimeMillis();
            trainTransferParamDTO.setDate(formatSimple.format(new Date(nowTime)));
            trainTransferParamDTO.setTimestamp(nowTime+"");
            trainTransferParamDTO.setFrom("shanghai"+random.nextInt(5));
            trainTransferParamDTO.setTo("beijing"+random.nextInt(5));
            trainTransferParamDTO.setStartDate(formatter.format(new Date(nowTime)));
            String data =JSONObject.toJSONString(trainTransferParamDTO);
            producer.send(new ProducerRecord<String,String>(TOPIC,key,data));
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
        new KafkaProducerTrain().produce();
    }
}
