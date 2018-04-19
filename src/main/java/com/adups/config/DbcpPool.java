package com.adups.config;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.spark.SparkFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author allen
 * @date 09/02/2018.
 */
public class DbcpPool implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(DbcpPool.class);

	private static Properties properties = new Properties();

	private static DataSource dataSource;

	static {
		try {
			InputStream inputStream = new FileInputStream(new File(SparkFiles.get("application.properties")));
			properties.load(inputStream);
			dataSource= BasicDataSourceFactory.createDataSource(properties);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}catch (Exception e){
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public static Connection getConnection(){
		Connection connection=null;
		try{
			connection=dataSource.getConnection();
		}catch (SQLException e){
			e.printStackTrace();
		}
		return connection;
	}
}
