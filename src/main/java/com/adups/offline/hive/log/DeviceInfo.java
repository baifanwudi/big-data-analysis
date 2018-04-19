package com.adups.offline.hive.log;

import com.adups.base.AbstractSparkSql;
import com.adups.common.ReadTable;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 25/07/2017.
 */
public class DeviceInfo extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum = 1;
		String beginTime=pt+" 00:00:00";
		String endTime=pt+" 23:59:59";
		String where="(select * from iot_register.device_info where create_time between '"+beginTime+"' and '"+endTime+"' ) as device_time_filter" ;

		Dataset<Row> deviceInfo=new ReadTable().loadTable(spark,where).repartition(partitionNum);
		deviceInfo.createOrReplaceTempView("deviceInfo");
		beforePartition(spark);
		String sql = "insert overwrite table device_info partition(pt='" + pt + "') " +
				"select product_id,mid,device_id,device_secret,ip,mac,oem,models,platform," +
				"device_type,version, appversion,sdkversion,continent_en,continent_zh,country_en," +
				"country_zh,province_en,province_zh,city_en,city_zh,networkType,create_time,data_type " +
				"from deviceInfo";
		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		DeviceInfo deviceInfo = new DeviceInfo();
		deviceInfo.runAll(pt);
	}

}
