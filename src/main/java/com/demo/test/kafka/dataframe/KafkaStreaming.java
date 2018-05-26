package com.demo.test.kafka.dataframe;

import com.demo.bean.input.schema.CheckSchema;
import com.demo.bean.input.schema.DownSchema;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;


import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Created by allen on 22/08/2017.
 */
public class KafkaStreaming {
	public static void main(String[] args) throws StreamingQueryException {
		Map<String, Object> kafkaParams=new HashMap<>();
		kafkaParams.put("bootstrap.servers", "180.97.69.208:19090,180.97.69.210:19090,180.97.69.211:19090");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		SparkSession spark=SparkSession.builder().appName("test").master("local[10]").getOrCreate();

		String bootstrapServers = "180.97.69.208:19090,180.97.69.210:19090,180.97.69.211:19090";
		String checkTopics = "ota_check";
		Dataset<Row> checkData= spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", checkTopics)
				.option("startingOffsets", "latest")  //latest,earliest
				.option("maxPartitions", 10)
				.option("kafkaConsumer.pollTimeoutMs", 512)
				.option("failOnDataLoss", false).load()
				.withWatermark("timestamp","10 minutes")
				.selectExpr("CAST(value as String)");

		String downTopics="ota_download";
		Dataset<Row> downDate= spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", downTopics)
				.option("startingOffsets", "latest")  //latest,earliest
				.option("maxPartitions", 10)
				.option("kafkaConsumer.pollTimeoutMs", 512)
				.option("failOnDataLoss", false).load()
				.withWatermark("timestamp","10 minutes")
				.selectExpr("CAST(value as String)");

		StructType checkSchema=new CheckSchema().produceSchema();
		Dataset<Row> otaCheck = checkData.select(functions.from_json(col("value"), checkSchema).as("otacheck"))
				.select("otacheck.*");

		Dataset<Row> otaCheckFilter=otaCheck.groupBy(
				window(col("createTime"),"10 minutes"),col("productId")
		).agg(count(col("deviceId")).as("newNum"));

		StructType downSchema=new DownSchema().produceSchema();
		Dataset<Row> otaDown=downDate.select(functions.from_json(col("value"), downSchema).as("otadown")).select("otadown.*");

		Dataset<Row>  otaDownFilter=otaDown.groupBy(
				window(col("createTime"),"10 minutes"),col("productId")
		).agg(count(col("deviceId")).as("newNum"));

		//TODO spark api 暂时不支持 dateset Streaming 之间 join
		Dataset<Row> result=otaCheckFilter.join(otaDownFilter);

//		otaprecord.show();

//		otaprecord.printSchema();
		StreamingQuery query=result.writeStream().outputMode("complete").format("console").start();

		query.awaitTermination();

	}

//	public static StructType produceCheckSchema(){
//		List<StructField> inputFields=new ArrayList<>();
//		String stringType="networkType,lac,cid,mcc,mnc,rxlev,mid,deviceId,originVersion,nowVersion,ip,province,city";
//		String timeType="createTime";
//		String longType="productId,deltaId";
//		for(String stringTmp:stringType.split(",")){
//			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
//		}
//		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,true));
//		for(String longTmp:longType.split(",")){
//			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
//		}
//		return DataTypes.createStructType(inputFields);
//	}
//
//	public static  StructType produceDownSchema(){
//		List<StructField> inputFields=new ArrayList<>();
//		String stringType="downStart,downEnd,apnType,extStr,downIp,mid,deviceId,originVersion,nowVersion,ip,province,city";
//		String timeType="createTime";
//		String integerType="downloadStatus,downSize";
//		String longType="productId,deltaId";
//		for(String stringTmp:stringType.split(",")){
//			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
//		}
//		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,false));
//		for(String integerTmp:integerType.split(",")){
//			inputFields.add(DataTypes.createStructField(integerTmp,DataTypes.IntegerType,true));
//		}
//		for(String longTmp:longType.split(",")){
//			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
//		}
//		return DataTypes.createStructType(inputFields);
//	}
}
