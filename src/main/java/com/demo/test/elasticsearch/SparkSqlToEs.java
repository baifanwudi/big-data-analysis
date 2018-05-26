package com.demo.test.elasticsearch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

/**
 * @author allen
 * @date 2018/5/9.
 */
public class SparkSqlToEs {

	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder().appName("Sql2Es").config("es.nodes","localhost")
				.config("es.port","9200").master("local[3]").getOrCreate();

		Dataset<Row> person=spark.read().json("/opt/spark/examples/src/main/resources/people.json");

		person.show();

		JavaEsSparkSQL.saveToEs(person,"test/person");

	}
}
