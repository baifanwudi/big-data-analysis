package com.adups.test.bak.logfilter;

import com.adups.base.AbstractSparkSql;
import com.adups.config.FlumePath;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import  static org.apache.spark.sql.functions.*;
import java.io.IOException;

/**
 * @author allen on 25/07/2017.
 */
public class DeviceInfoFilter extends AbstractSparkSql {


	private Logger logger = LoggerFactory.getLogger(this.getClass());
	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum = 1;
		String ptPre = DateUtil.pathPtWithPre(pt);
		String devicePath = FlumePath.DEVICE_INFO_PATH+ ptPre;
		if(existsPath(devicePath)!=true){
			return;
		}

		Dataset<Row> deviceInfo = spark.read().parquet(devicePath).filter(col("create_time").like(pt+"%"))
				.filter(not(col("productId").isin(ProductLogFilter.filterProduct)))
						.distinct().repartition(partitionNum);

		if(deviceInfo.count()==0){
			logger.warn("the num is 0");
			return ;
		}
		deviceInfo.createOrReplaceTempView("deviceInfo");

		spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;" +
				" set hive.exec.dynamic.partition=true; set hive.exec.max.dynamic.partitions.pernode=3000;");

		String sql = "insert overwrite table ct_iot.device_info partition(pt='" + pt + "') " +
				"select product_id,mid,device_id,device_secret,ip,mac,oem,models,platform," +
				"device_type,version, appversion,sdkversion,continent_en,continent_zh,country_en," +
				"country_zh,province_en,province_zh,city_en,city_zh,networkType,create_time from deviceInfo";

		logger.warn("executing sql is :" + sql);

		spark.sql(sql);
	}


	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		DeviceInfoFilter deviceInfoFilter = new DeviceInfoFilter();
		deviceInfoFilter.runAll(pt);

	}

}
