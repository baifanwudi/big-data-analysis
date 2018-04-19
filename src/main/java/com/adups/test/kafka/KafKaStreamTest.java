package com.adups.test.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;

import java.util.*;

/**
 * Created by allen on 22/08/2017.
 */
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class KafKaStreamTest {

	public void runAll() throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "180.97.69.208:19090,180.97.69.210:19090,180.97.69.211:19090");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "localTest");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		//.setMaster("local[4]")
		SparkConf sparkConf = new SparkConf().setAppName("spark-streaming");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		jssc.checkpoint("checkPointTmp");

		Collection<String> checkTopics = Arrays.asList("ota_check");

		Collection<String> downTopics = Arrays.asList("ota_download");

		JavaInputDStream<ConsumerRecord<String, String>> streamCheck = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(checkTopics, kafkaParams));


//		finalResult.foreachRDD(
//				rdd->{
//					rdd.foreachPartition(partitionOfRecord ->{
//						//jdbc Pool
//						/*
//						Connection connection = ConnectionPool.getConnection();
//						while (partitionOfRecords.hasNext()) {
//							connection.send(partitionOfRecords.next());
//						}
//						ConnectionPool.returnConnection(connection); // return to the pool for future reuse
//						 */
//					});
//				}
//		);

//
//		result.print();

//		JavaPairDStream<String,Integer> test=downResult.groupByKey().mapValues(
//				(set)->{
//					Set<String> setList=new HashSet<>();
//					for(String tmp:set){
//						setList.add(tmp);
//					}
//					return setList.size();
//				}
//		);
//		test.window(Durations.minutes(5), Durations.minutes(1));
//		test.print();
//		Function2<String,String,String>
//		JavaPairDStream<Tuple4, Tuple2> test2 = checkResult;
//		JavaPairDStream<Tuple4,Double>  test2=downResult;
//
//		test2.print();
//		result.groupByKey().map(
//
//		);


//	    lines.print();
//		result.print();


		//做了一个 distinct
//		checkResult.mapToPair(
//				s -> {
//					return new Tuple2<>(s, 1);
//				}
//		).reduceByKey((i1, i2) -> i1 + i2).mapToPair(
//				s -> {
//					return new Tuple2<>(s._1._1(), s._1._2);
//				}
//		);
		jssc.start();
		jssc.awaitTermination();


	}


	public static void main(String[] args) throws InterruptedException {
		KafKaStreamTest kafKaStreamTest = new KafKaStreamTest();
		kafKaStreamTest.runAll();
	}
}
