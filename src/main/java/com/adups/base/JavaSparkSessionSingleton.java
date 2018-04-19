package com.adups.base;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author allen
 * Created on 05/12/2017.
 */
public class JavaSparkSessionSingleton {

	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		return instance;
	}
}
