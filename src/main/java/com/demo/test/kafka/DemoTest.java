package com.demo.test.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author allen
 * @date 2018/5/29.
 */
public class DemoTest {

	public static void main(String[] args) {

		SparkSession spark=SparkSession.builder().appName("app-train").master("local[*]").getOrCreate();

		Dataset<Row> result=spark.read().json("");
	}
}
