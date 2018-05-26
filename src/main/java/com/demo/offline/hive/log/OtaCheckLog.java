package com.demo.offline.hive.log;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.input.schema.CheckSchema;
import com.demo.config.FlumePath;
import com.demo.util.DateUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 25/07/2017.
 */
public class OtaCheckLog extends AbstractSparkSql {

	private  Logger logger = LoggerFactory.getLogger(OtaCheckLog.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum=1;
		String checkPath= FlumePath.CHECK_PATH+DateUtil.pathPtWithPre(pt);
		if(!existsPath(checkPath)){
			return;
		}
		StructType checkSchema=new CheckSchema().produceSchema();
		Dataset<Row> otaCHeck=spark.read().schema(checkSchema).json(checkPath).repartition(partitionNum);
		otaCHeck.createOrReplaceTempView("otaCheck");
//		插入分区表前缀
		beforePartition(spark);
		String sql= "insert overwrite table ota_interface_check_info_log partition(pt='"+pt+"')" +
				" select mid,deviceId,productId,deltaId,originVersion,nowVersion,createTime,ip,province,city,networkType," +
				"lac,cid,mcc,mnc,rxlev,dataType from otaCheck";
		logger.warn("executing sql is :"+sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {

		String pt= DateUtil.producePtOrYesterday(args);
		OtaCheckLog otaCheckLog =new OtaCheckLog();
		otaCheckLog.runAll(pt);
	}

}
