package com.adups.test.bak.logfilter;

import com.adups.base.AbstractSparkSql;
import com.adups.config.FlumePath;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

/**
 * @author allen on 25/07/2017.
 */
public class OtaDownloadLogFilter extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(OtaDownloadLogFilter.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum = 1;

		String downPath = FlumePath.DOWN_PATH + DateUtil.pathPtWithPre(pt);

		if(existsPath(downPath)!=true ) {
			return;
		}
		//TODO apn_type默认为0,以后有值再修改
		Dataset<Row> otaDownload = spark.read().schema(produceSchema()).json(downPath)
				.filter(col("createTime").like(pt+"%"))
				.filter(not(col("productId").isin(ProductLogFilter.filterProduct))).distinct()
				.na().fill("0", CommonUtil.columnNames("apnType")).repartition(partitionNum);


		if(otaDownload.count()==0){
			logger.warn("the num is 0");
			return ;
		}

		otaDownload.createOrReplaceTempView("otaDownload");

		spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;" +
				" set hive.exec.dynamic.partition=true; set hive.exec.max.dynamic.partitions.pernode=3000;");

		String sql = "insert overwrite table ct_iot.ota_interface_download_info_log partition(pt='"+pt+"') " +
				"select mid,deviceId,productId,originVersion,nowVersion,downStart,downEnd,deltaId,downloadStatus,createTime,ip," +
				"province,apnType,city, extStr,downSize,downIp from otaDownload";

		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public StructType produceSchema(){
		List<StructField> inputFields=new ArrayList<>();
		String stringType="downStart,downEnd,apnType,extStr,downIp,mid,deviceId,originVersion,nowVersion,ip,province,city";
		String timeType="createTime";
		String integerType="downloadStatus,downSize";
		String longType="productId,deltaId";
		for(String stringTmp:stringType.split(",")){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,true));
		for(String integerTmp:integerType.split(",")){
			inputFields.add(DataTypes.createStructField(integerTmp,DataTypes.IntegerType,true));
		}
		for(String longTmp:longType.split(",")){
			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,true));
		}
		return DataTypes.createStructType(inputFields);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaDownloadLogFilter deviceInfo = new OtaDownloadLogFilter();
		deviceInfo.runAll(pt);

	}

}
