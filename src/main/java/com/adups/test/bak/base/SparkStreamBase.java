package com.adups.test.bak.base;

import com.adups.config.KafkaConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author allen
 * Created by allen on 11/09/2017.
 */
public abstract class SparkStreamBase {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public void executeSpark(String topics) throws Exception {
		if(topics==null ||topics.equals("")){
			logger.error("topics cant be null or \"\"");
			return ;
		}
		String appName=this.getClass().getSimpleName()+":"+topics;
		SparkSession spark = SparkSession.builder().appName(appName).getOrCreate();
		String bootstrapServers = KafkaConfig.KAFKA_BROKER_LIST;
		logger.warn("spark streaming is executing the topic of "+topics);
		Dataset<Row> dataset = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", topics)
				.option("startingOffsets", "latest")  //latest,earliest
				.option("kafkaConsumer.pollTimeoutMs", 512)
				.option("failOnDataLoss", true).load()
				.selectExpr("CAST(value as String)");

		executeStreaming(dataset,topics);
	}

	public abstract void  executeStreaming(Dataset<Row> dataset,String topics) throws Exception;

	public void runAll(String topics) throws Exception {
		executeSpark(topics);
	}
}
