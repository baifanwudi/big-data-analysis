package com.adups.test.bak.logfilter;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

/**
 * @author allen on 25/07/2017.
 */
public class OtaCheckLogFilter extends AbstractSparkSql {

	private  Logger logger = LoggerFactory.getLogger(OtaCheckLogFilter.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		int partitionNum=1;

		String checkPath= FlumePath.CHECK_PATH+DateUtil.pathPtWithPre(pt);

		if(existsPath(checkPath)!=true){
			return;
		}

		Dataset<Row> otaCHeck=spark.read().schema(produceSchema()).json(checkPath).filter(col("createTime").like(pt+"%"))
				.filter(not(col("productId").isin(ProductLogFilter.filterProduct))).repartition(partitionNum);


		if(otaCHeck.count()==0){
			logger.warn("the num is 0");
			return ;
		}
		otaCHeck.createOrReplaceTempView("otaCheck");

//		插入分区表前缀
		spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;" +
				" set hive.exec.dynamic.partition=true; set hive.exec.max.dynamic.partitions.pernode=3000;");

		String sql= "insert overwrite table ct_iot.ota_interface_check_info_log partition(pt='"+pt+"')" +
				" select mid,deviceId,productId,deltaId,originVersion,nowVersion,createTime,ip,province,city,networkType,lac,cid,mcc,mnc,rxlev from otaCheck";

		logger.warn("executing sql is :"+sql);

		spark.sql(sql);

	}

	public StructType produceSchema(){
		List<StructField> inputFields=new ArrayList<>();
		String stringType="networkType,lac,cid,mcc,mnc,rxlev,mid,deviceId,originVersion,nowVersion,ip,province,city";
		String timeType="createTime";
		String longType="productId,deltaId";
		for(String stringTmp:stringType.split(",")){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,true));
		for(String longTmp:longType.split(",")){
			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,true));
		}
		return DataTypes.createStructType(inputFields);
	}


	public static void main(String[] args) throws IOException {

		String pt= DateUtil.producePtOrYesterday(args);
		OtaCheckLogFilter otaCheckLog =new OtaCheckLogFilter();
		otaCheckLog.runAll(pt);

	}

}
