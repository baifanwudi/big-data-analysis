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


public class KafkaProducerDownload {


    /*
        java -Djava.ext.dirs=/usr/lib/kafka/libs/ -jar BigData.jar kafka.KafkaProducerDemo
     */
    private final Producer<String,String> producer;
    public final static String TOPIC="ota_download";

    private KafkaProducerDownload(){
        Properties props=new Properties();
        props.put("metadata.broker.list","180.97.69.50:19090,180.97.69.210:19090,180.97.69.211:19090");
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks","-1");

        producer=new Producer<String,String>(new ProducerConfig(props));
    }

    void produce(){
        int messageNo=1000;
        final int COUNT=10000;
        Random random=new Random();
        DownloadInfo downloadInfo=new DownloadInfo();
        downloadInfo.setDownloadStatus(1);
        downloadInfo.setMid("e0aee11a");
        downloadInfo.setDeltaId((long)14);
        downloadInfo.setProductId((long)1500003696);
//        downloadInfo.setDownEnd("1505007886");
//        downloadInfo.setDownStart("1505007880");
//        downloadInfo.setDownSize(9544647);
        while(messageNo<COUNT){
            String key=String.valueOf(messageNo);
            downloadInfo.setDeviceId(random.nextInt(6)+"test");
            Integer start=random.nextInt(100);
            Integer end=start+random.nextInt(100);
            downloadInfo.setDownEnd(end.toString());
            downloadInfo.setDownStart(start.toString());
            downloadInfo.setDownSize(random.nextInt(10)*100*1014);
            String json= JSONObject.toJSONString(downloadInfo);
//            String data=" allen Hello send kafka message "+key;
            String data=json;
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
        new KafkaProducerDownload().produce();
    }
}
