package com.adups.offline.hive.log;

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
 * Created by allen on 04/08/2017.
 */
public class OtaAppLog extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(OtaAppLog.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		int partitionNum = 4;
		String ptWithPre= DateUtil.pathPtWithPre(pt);
		String appLogPath= FlumePath.APP_LOG_PATH+ptWithPre;
		if(!existsPath(appLogPath)){
			return;
		}

		Dataset<Row> otaAppLog= spark.read().schema(produceSchema()).json(appLogPath).distinct().repartition(partitionNum);
		otaAppLog.createOrReplaceTempView("OtaAppLog");
		beforePartition(spark);
		String sql = "insert overwrite table ota_app_log partition(pt='"+pt+"') " +
				"select mid,ip,version,deviceId,productId,continentEn,continentZh,countryEn,countryZh,provinceEn,provinceZh,cityEn,cityZh," +
				"networktype,lac,cid,mcc,mnc,rxlev,num,goType,createTime,dataType from OtaAppLog";
		logger.warn("executing sql is :" + sql);
		spark.sql(sql);
	}

	public StructType produceSchema(){
		List<StructField> inputFields=new ArrayList<>();
		String splitSeq=",";
		String stringType="mid,ip,version,continentEn,continentZh,countryEn,countryZh,provinceEn,provinceZh," +
				"cityEn,cityZh,networktype,deviceId,lac,cid,mcc,mnc,rxlev,dataType";
		String timeType="createTime";
		String longType="productId";
		String integerType="num,goType";
		for(String stringTmp:stringType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,false));
		for(String integerTmp:integerType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(integerTmp,DataTypes.IntegerType,true));
		}
		for(String longTmp:longType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
		}
		return DataTypes.createStructType(inputFields);
	}

	public static void main(String[] args) throws Exception {
		String pt= DateUtil.producePtOrYesterday(args);
		OtaAppLog otaAppLog =new OtaAppLog();
		otaAppLog.runAll(pt);
	}
}
