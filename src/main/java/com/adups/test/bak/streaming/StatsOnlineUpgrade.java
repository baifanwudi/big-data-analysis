package com.adups.test.bak.streaming;

import com.adups.test.bak.base.SparkStreamBase;
import com.adups.bean.input.schema.UpgradeSchema;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.window;

/**
 * Created by allen on 07/09/2017.
 */
public class StatsOnlineUpgrade  extends SparkStreamBase {

	@Override
	public void executeStreaming(Dataset<Row> dataset, String topics) throws Exception {
		StructType upgradeSchema=new UpgradeSchema().produceSchema();
		Dataset<Row> otaCheck = dataset.select(functions.from_json(col("value"),upgradeSchema)
				.as("ota_upgrade")).select("ota_upgrade.*").repartition(10);

		Encoder<OnlineBean> onlineBeanEncoder=new OnlineBean().produceBeanEncoder();
		Dataset<OnlineBean> result=otaCheck.withWatermark("createTime","3 minutes").filter(col("updateStatus").equalTo(1)).
				groupBy(window(col("createTime"),"1 minute"),
				col("productId"),col("deltaId"),col("originVersion"),
				col("nowVersion")).count().withColumnRenamed("count","num")
				.withColumn("topic",lit("upgrade")).as(onlineBeanEncoder).repartition(2);

		StreamingQuery query = result.writeStream()
				.foreach(new OnlineSaveTest()).outputMode("append")
				.option("checkpointLocation",topics+"checkPoint")
				.trigger(ProcessingTime.create(1, TimeUnit.MINUTES))
				.start();

		query.awaitTermination();
	}

	public static void main(String[] args) throws Exception {

		String topics = "ota_upgrade";
		StatsOnlineUpgrade statsOnlineUpgrade=new StatsOnlineUpgrade();
		statsOnlineUpgrade.runAll(topics);

	}



}
