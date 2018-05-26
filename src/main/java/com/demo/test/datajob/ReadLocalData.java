package com.demo.test.datajob;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static  org.apache.spark.sql.functions.countDistinct;

/**
 * Created by allen on 13/07/2017.
 */
public class ReadLocalData {


	public static void main(String[] args) {

		SparkSession spark=SparkSession
				.builder().appName("Read Initialize").master("local[2]").getOrCreate();

//		Dataset<Row> orgin=spark.read().parquet("file_result/testfile.parquet");


		Dataset<Row> orgin=spark.read().parquet("file_result/pt=2017-07-11/");

		Dataset<Row> result =orgin.groupBy("now_version")

				.agg(countDistinct("origin_version","create_time").as("count"));

		orgin.show();

//		result.show();

		spark.stop();


	}
}
