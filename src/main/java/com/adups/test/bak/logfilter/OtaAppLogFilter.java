package com.adups.test.bak.logfilter;

import com.adups.base.AbstractSparkSql;
import com.adups.config.FlumePath;
import com.adups.config.HiveConfig;
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
 * @author allen on 04/08/2017.
 */
public class OtaAppLogFilter extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(OtaAppLogFilter.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		int partitionNum = 1;

		String ptWithPre= DateUtil.pathPtWithPre(pt);

		String appLogPath = FlumePath.APP_LOG_PATH+ ptWithPre;

		if(existsPath(appLogPath)!=true){
			return;
		}

		Dataset<Row> otaAppLog= spark.read().schema(produceSchema()).json(appLogPath)
				.filter(col("createTime").like(pt+"%"))
				.filter(not(col("productId").isin(ProductLogFilter.filterProduct))).distinct().repartition(partitionNum);

		if(otaAppLog.count()==0){
			logger.warn("the num is 0");
			return ;
		}
		otaAppLog.createOrReplaceTempView("OtaAppLog");

		spark.sql(HiveConfig.SQL_DATABASE);
		spark.sql(HiveConfig.HIVE_PARTITION);

		String sql = "insert overwrite table ota_app_log partition(pt='"+pt+"') " +
				"select mid,ip,version,deviceId,productId,continentEn,continentZh,countryEn,countryZh,provinceEn,provinceZh,cityEn,cityZh," +
				"networktype,lac,cid,mcc,mnc,rxlev,num,goType,createTime from OtaAppLog";

		logger.warn("executing sql is :" + sql);

		spark.sql(sql);
	}

	public StructType produceSchema(){
		List<StructField> inputFields=new ArrayList<>();
		String stringType="mid,ip,version,continentEn,continentZh,countryEn,countryZh,provinceEn,provinceZh," +
				"cityEn,cityZh,networktype,deviceId,lac,cid,mcc,mnc,rxlev";
		String timeType="createTime";
		String longType="productId";
		String integerType="num,goType";
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


	public static void main(String[] args) throws Exception {
		String pt= DateUtil.producePtOrYesterday(args);
		OtaAppLogFilter otaAppLog =new OtaAppLogFilter();
		otaAppLog.runAll(pt);
	}
}
