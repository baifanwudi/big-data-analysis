package com.demo.offline.hive.log;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.input.schema.DownSchema;
import com.demo.config.FlumePath;
import com.demo.util.CommonUtil;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 25/07/2017.
 */
public class OtaDownloadLog extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(OtaDownloadLog.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum = 1;
		String downPath = FlumePath.DOWN_PATH + DateUtil.pathPtWithPre(pt);
		if(!existsPath(downPath)) {
			return;
		}
		// apn_type默认为0,以后有值再修改
		StructType downSchema= new DownSchema().produceSchema();
		Dataset<Row> otaDownload = spark.read().schema(downSchema).json(downPath).distinct()
				.na().fill("0", CommonUtil.columnNames("apnType")).repartition(partitionNum);
		otaDownload.createOrReplaceTempView("otaDownload");
		beforePartition(spark);
		String sql = "insert overwrite table ota_interface_download_info_log partition(pt='"+pt+"') " +
				"select mid,deviceId,productId,originVersion,nowVersion,downStart,downEnd,deltaId,downloadStatus,createTime,ip," +
				"province,apnType,city, extStr,downSize,downIp ,dataType from otaDownload";
		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaDownloadLog otaDownloadLog = new OtaDownloadLog();
		otaDownloadLog.runAll(pt);
	}

}
