package com.demo.test.kafka.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.col;

/**
 * Created by allen on 13/07/2017.
 */
public class KafkaStructStream {
	public static void main(String[] args) throws StreamingQueryException {

//	Encoder<OtaPreRecord> otaPreRecordEncoders=Encoders.bean(OtaPreRecord.class);

		StructType schema = new ConstructType().produceSchema();

		SparkSession spark = SparkSession.builder().appName("KafKa Initialize").master("local[2]").getOrCreate();

		String bootstrapServers = "180.97.69.208:19090";
		String topics = "test";

		Dataset<Row> dataset = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", topics)
				.option("startingOffsets", "latest")  //latest,earliest
				.option("maxPartitions", 10)
				.option("kafkaConsumer.pollTimeoutMs", 512)
				.option("failOnDataLoss", false).load()
				.selectExpr("CAST(value as String)");


		Dataset<Row> otaprecord = dataset.select(functions.from_json(col("value"), schema).as("otaprecord"));
		Dataset<Row> allColumns = otaprecord.select("otaprecord.*");

		Column ptColumn = functions.date_format(col("create_time").cast("timestamp"), "yyyy-MM-dd").as("pt");

		Dataset<Row> resultWithPt = allColumns.select(col("*"), ptColumn);


		StreamingQuery query = resultWithPt.writeStream()
				.outputMode("append").format("console").start();


		StreamingQuery saveResult = resultWithPt.writeStream().format("parquet").partitionBy("pt")
				.option("path", "file_result")
				.option("checkpointLocation", "checkpointPath").trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
				.outputMode("append").start();

		//		Dataset<OtaPreRecord>  finaltmp=tmp.select("otaprecord.*").as(otaPreRecordEncoders);

//		query.awaitTermination();


		saveResult.awaitTermination();


	}


}
