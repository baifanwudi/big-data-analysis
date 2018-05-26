package com.demo.config;

/**
 * @author allen
 * Created by allen on 17/08/2017.
 */
public class HiveConfig {

	public  static final String HIVE_DATABASE=PropertiesConfig.HIVE_DATABASE;

	public static final String SQL_DATABASE="use "+HIVE_DATABASE;

	public static final String HIVE_PARTITION="set hive.exec.dynamic.partition.mode=nonstrict;" +
			" set hive.exec.dynamic.partition=true; set hive.exec.max.dynamic.partitions=3000;";

}
