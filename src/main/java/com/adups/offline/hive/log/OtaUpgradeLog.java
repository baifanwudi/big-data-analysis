package com.adups.offline.hive.log;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.input.schema.UpgradeSchema;
import com.adups.config.FlumePath;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
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
public class OtaUpgradeLog extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(OtaUpgradeLog.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		int partitionNum = 1;
		String upgradePath = FlumePath.UPGRADE_PATH + DateUtil.pathPtWithPre(pt);
		if(!existsPath(upgradePath)){
			return;
		}
		//apn_type默认为0,以后有值再修改
		StructType upgradeSchema=new UpgradeSchema().produceSchema();
		Dataset<Row> otaUpgrade = spark.read().schema(upgradeSchema).json(upgradePath).distinct().na().fill("0", CommonUtil.columnNames("apnType")).
				repartition(partitionNum);
		otaUpgrade.createOrReplaceTempView("otaUpgrade");
		beforePartition(spark);
		String sql = "insert overwrite table ota_interface_upgrade_info_log partition(pt='" + pt + "') " +
				"select mid,deviceId,productId,originVersion,nowVersion,updateStatus,deltaId,ip,province,apnType," +
				"city,extStr,createTime ,dataType from otaUpgrade";
		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaUpgradeLog otaUpgradeLog = new OtaUpgradeLog();
		otaUpgradeLog.runAll(pt);
	}

}
