package com.demo.common;

import com.demo.config.PropertiesConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author allen
 * @date 02/11/2017.
 */
public class ReadTable {

	public Dataset<Row> loadTable( SparkSession spark,String tableName){

		Dataset<Row> jdbcTable=spark.read().format("jdbc")
				.option("url", PropertiesConfig.URL)
				.option("dbtable",tableName)
				.option("user",PropertiesConfig.USERNAME)
				.option("password",PropertiesConfig.PASSWORD).load();

		return jdbcTable;
	}

}
