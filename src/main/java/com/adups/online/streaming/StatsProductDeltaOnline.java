package com.adups.online.streaming;

import com.adups.base.AbstractKafkaStreaming;
import com.adups.common.sql.streaming.ProductDeltaOnlineSave;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import scala.Tuple5;
import java.util.*;

/**
 * @author allen
 * 执行命令
 * $SPARK_HOME/bin/spark-submit  --class "com.adups.online.streaming.StatsProductDeltaOnline"
 * --total-executor-cores 5 --packages com.alibaba:fastjson:1.2.11,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1
 * BigDataAnalysis-1.0.0.jar streamingCheckpoint
 */
public class StatsProductDeltaOnline extends AbstractKafkaStreaming{

	@Override
	public JavaStreamingContext createContext(JavaStreamingContext javaStreamingContext, Map<String, Object> kafkaParams) {

		kafkaParams.put("group.id", "online");
		String checkTopic="ota_check";
		String downTopic="ota_download";
		String upgradeTopic="ota_upgrade";

		Collection<String> topics = Arrays.asList(checkTopic,downTopic,upgradeTopic);

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaPairDStream<Tuple5, Integer> result = stream.mapToPair(
				s -> {
					String json = s.value();
					String topic=s.topic();
					String type=null;
					try {
						if(checkTopic.equals(topic)){
							 type="check";
						}else if (downTopic.equals(topic)){
							Integer downloadStatus=JSONObject.parseObject(json).getInteger("downloadStatus");
							if(downloadStatus!=1){
								return new Tuple2<Tuple5, Integer>(null, null);
							}
							type="down";
						}else if(upgradeTopic.equals(topic)){
							Integer updateStatus=JSONObject.parseObject(json).getInteger("updateStatus");
							if(updateStatus!=1){
								return new Tuple2<Tuple5, Integer>(null, null);
							}
							type="upgrade";
						}
						Long productId = JSONObject.parseObject(json).getLong("productId");
						Long deltaId = JSONObject.parseObject(json).getLong("deltaId");
						String originVersion = JSONObject.parseObject(json).getString("originVersion");
						String nowVersion= JSONObject.parseObject(json).getString("nowVersion");
						Tuple5<Long, Long, String, String,String> tuple5 = new Tuple5<Long, Long, String, String,String>(productId, deltaId,originVersion,nowVersion,type);
						return new Tuple2<Tuple5, Integer>(tuple5, 1);
					} catch (Exception e) {
						return new Tuple2<Tuple5, Integer>(null, null);
					}
				}
		).filter(s->s._2()!=null ).reduceByKey((s1,s2)-> s1+s2).repartition(2);

		result.foreachRDD(
				rdd->{
					rdd.foreachPartition(partitionOfRecord ->{
						String sql="insert into stats_online_check_down_upgrade(product_id,delta_id,origin_version," +
								"target_version,type,num) values (?,?,?,?,?,?)";
						new ProductDeltaOnlineSave().putDataBatch(partitionOfRecord,sql);
					});
				}
		);

		return javaStreamingContext;
	}

	public static void main(String[] args) throws InterruptedException {

		String checkpointDirectory=args[0];
		StatsProductDeltaOnline statsProductDeltaOnline =new StatsProductDeltaOnline();
		statsProductDeltaOnline.runAll(checkpointDirectory);
	}
}
