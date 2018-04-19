package com.adups.test.bak.streaming;

import com.adups.test.bak.base.SparkStreamBase;
import com.adups.bean.input.schema.CheckSchema;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

/**
 * Created by allen on 07/09/2017.
 */
public class StatsOnlineCheck  extends SparkStreamBase{

	@Override
	public void executeStreaming(Dataset<Row> dataset,String topics) throws StreamingQueryException {
			StructType checkSchema=new CheckSchema().produceSchema();
			Dataset<Row> otaCheck = dataset.select(functions.from_json(col("value"),
					checkSchema).as("ota_check")).select("ota_check.*").repartition(4);

		Encoder<OnlineBean> onlineBeanEncoder=new OnlineBean().produceBeanEncoder();


		Dataset<OnlineBean>  check=	otaCheck.withWatermark("createTime","1 minute")
				.groupBy(window(col("createTime"),"1 minute","1 minute"),col("productId"),
				col("deltaId"),col("originVersion"),col("nowVersion"))
				.agg(count(col("deviceId")).as("num"))
				.withColumn("type",lit("check"))
				.withColumn("createTime",current_timestamp()).orderBy("window")
				.as(onlineBeanEncoder).repartition(1);



//		StreamingQuery query = check.writeStream()
//				.foreach(new OnlineSaveTest()).outputMode("complete")
//				.trigger(ProcessingTime.create(1, TimeUnit.MINUTES))
//				.start();
		//
		StreamingQuery query=check.writeStream().format("console").outputMode("update")
				.trigger(ProcessingTime.create(1, TimeUnit.MINUTES)).start();

		query.awaitTermination();
	}

	public static void main(String[] args) throws Exception {
//		String topics="ota_check";
		String topics="test";
		StatsOnlineCheck statsOnlineCheck=new StatsOnlineCheck();
		statsOnlineCheck.runAll(topics);
	}




}
