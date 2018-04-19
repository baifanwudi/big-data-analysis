package com.adups.offline.hive.app;

import com.adups.base.AbstractSparkSql;
import com.adups.config.FlumePath;
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

/**
 * @author allen
 * @date 27/02/2018.
 */
public class AppCheckLog extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum =1;
		String ptWithPre= DateUtil.pathPtWithPre(pt);
		String appCheckLogPath= FlumePath.APP_CHECK_LOG_PATH+ptWithPre;
		if(!existsPath(appCheckLogPath)){
			return;
		}

		Dataset<Row> appCheckAll= spark.read().schema(produceSchema()).json(appCheckLogPath).distinct().repartition(partitionNum);
		appCheckAll.createOrReplaceTempView("appCheckLog");

		beforePartition(spark);
		String sql = "insert overwrite table app_check_log partition(pt='"+pt+"') " +
				"select mid,deviceId,productId,ip,appName,packageName,versionCode,versionName,continent_en,continent_zh,country_en,country_zh,province_en," +
				"province_zh,city_en,city_zh,createTime from appCheckLog where packageName!='' ";
		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public StructType produceSchema() {
		List<StructField> inputFields=new ArrayList<>();
		String splitSeq=",";
		String stringType="appName,versionName,mid,ip,sign,deviceId,productId," +
				"continent_en,continent_zh,country_en,country_zh,province_en,province_zh,city_en,city_zh,versionCode";
		String timeType="createTime";
		for(String stringTmp:stringType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		String packageNameType="packageName";
		inputFields.add(DataTypes.createStructField(packageNameType,DataTypes.StringType,false));

		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,true));
		return DataTypes.createStructType(inputFields);
	}


	public static void main(String[] args) throws IOException {
		String pt= DateUtil.producePtOrYesterday(args);
		AppCheckLog appCheckLog=new AppCheckLog();
		appCheckLog.runAll(pt);
	}
}
