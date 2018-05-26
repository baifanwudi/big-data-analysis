package com.demo.base;

import com.demo.config.KafkaConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import java.util.HashMap;
import java.util.Map;

/**
 * @author allen
 * Created on 05/12/2017.
 */
public abstract class AbstractKafkaStreaming {

	/**
	 * 对kafka读取内容处理,可以覆盖kafka参数
	 * @param javaStreamingContext
	 * @param kafkaParams
	 * @return
	 */
	public  abstract  JavaStreamingContext createContext(JavaStreamingContext javaStreamingContext,Map<String,Object> kafkaParams);


	private void executeStreaming(String checkpointDirectory,Boolean isLatest ,Duration duration)throws InterruptedException{

		String appName=this.getClass().getSimpleName();
		SparkConf sparkConf = new SparkConf().setAppName(appName);
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, duration);
		jssc.checkpoint(checkpointDirectory);

		Map<String, Object> kafkaParams = new HashMap<>(16);
		kafkaParams.put("bootstrap.servers", KafkaConfig.KAFKA_BROKER_LIST);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "online");
		if(isLatest){
			kafkaParams.put("auto.offset.reset", "latest");
		}else{
			kafkaParams.put("auto.offset.reset", "earliest");
		}
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("max.poll.records", 1000);

		Function0<JavaStreamingContext> createContextFunc= ()->createContext(jssc,kafkaParams);
		JavaStreamingContext ssc=JavaStreamingContext.getOrCreate(checkpointDirectory,createContextFunc);
		ssc.start();
		ssc.awaitTermination();
	}

	public  void runAll(String checkPointDirectory) throws InterruptedException {
		executeStreaming(checkPointDirectory,true,Durations.minutes(1));
	}


	public  void runAll(String checkPointDirectory,Duration duration) throws InterruptedException {
		executeStreaming(checkPointDirectory,true,duration);
	}

	public  void runAll(String checkPointDirectory,Boolean isLatest,Duration duration) throws InterruptedException {
		executeStreaming(checkPointDirectory,isLatest,duration);
	}
}
