package com.adups.config;

import org.apache.spark.SparkFiles;
import java.io.*;
import java.util.Properties;

/**
 * @author allen
 * Created by allen on 09/09/2017.
 */
public final class PropertiesConfig {

	private static Properties properties = new Properties();

	public static String URL;

	public static String USERNAME;

	public static String PASSWORD;

	public static String HIVE_DATABASE;

	public static String KAFKA_BROKER_LIST;

	public static String FLUME_PATH;

	static {
		try {
			InputStream inputStream = new FileInputStream(new File(SparkFiles.get("application.properties")));

			properties.load(inputStream);

			URL = properties.getProperty("url");

			USERNAME = properties.getProperty("username");

			PASSWORD = properties.getProperty("password");

			HIVE_DATABASE = properties.getProperty("hive.database");

			KAFKA_BROKER_LIST = properties.getProperty("kafka.bootstrap-servers");

			FLUME_PATH=properties.getProperty("flume.path");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
