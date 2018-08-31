package com.demo.test.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * @date 2018/8/11.
 */
public class PercentCountStats {

	public static void main(String[] args) {

		SparkSession spark=SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> table=spark.read().json("src/main/data/transfer_line_count.txt");

		table.printSchema();

		table.show();

		table.agg(sum("total_num")).show();

		Dataset<Row> result=table.withColumn("percent",format_number(col("total_num").divide( sum("total_num").over()).multiply(100),5));

		result.show();


	}
}
