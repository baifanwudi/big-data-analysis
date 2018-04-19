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
public class AppRegisterInfo extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum =1;
		String ptWithPre= DateUtil.pathPtWithPre(pt);
		String appRegisterInfoPath= FlumePath.APP_REGISTER_INFO_PATH+ptWithPre;
		if(!existsPath(appRegisterInfoPath)){
			return;
		}
		Dataset<Row> appRegisterInfo= spark.read().schema(produceSchema()).json(appRegisterInfoPath).distinct().repartition(partitionNum);
		appRegisterInfo.createOrReplaceTempView("appRegisterInfo");
		beforePartition(spark);
		String sql = "insert overwrite table app_register_info partition(pt='"+pt+"') " +
				"select mid,deviceId,productId,ip,appName,packageName,versionCode,versionName,continentEn," +
				"continentZh,countryEn,countryZh,provinceEn,provinceZh,cityEn,cityZh,createTime from appRegisterInfo where packageName!=''";
		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public StructType produceSchema() {
		List<StructField> inputFields=new ArrayList<>();
		String splitSeq=",";
		String stringType="mid,deviceId,productId,ip,appName,,versionCode,versionName,continentEn," +
				"continentZh,countryEn,countryZh,provinceEn,provinceZh,cityEn,cityZh";
		String timeType="createTime";
		String packageNameType="packageName";
		for(String stringTmp:stringType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		inputFields.add(DataTypes.createStructField(packageNameType,DataTypes.StringType,false));
		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,false));

		return DataTypes.createStructType(inputFields);
	}

	public static void main(String[] args) throws IOException {
		String pt= DateUtil.producePtOrYesterday(args);
		AppRegisterInfo appRegisterInfo=new AppRegisterInfo();
		appRegisterInfo.runAll(pt);
	}
}
