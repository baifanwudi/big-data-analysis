package com.adups.offline.hive.base;

import com.adups.config.OnlineOfflinePath;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 02/08/2017.
 */
public class OtaCheckBase extends AbstractTimeFilter {

	private Logger logger = LoggerFactory.getLogger(OtaCheckBase.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		parseOtaCheckLog(spark, pt);
		completeMissingData(spark, pt);
	}

	private void parseOtaCheckLog(SparkSession spark, String pt) {

		Dataset<Row> sqlResult = spark.sql("select * from ota_interface_check_info_log where pt='" +pt + "' and create_time is not null and" +
				" product_id is not null and now_version is not null and origin_version is not null");
		Dataset<Row> check=filterTimeIsNotPt(sqlResult,true,pt);
		if (check.count() == 0) {
			logger.error("the count of ota_interface_check_info_log in " + pt + " is 0 ");
			return;
		}
		WindowSpec w = Window.partitionBy("device_id", "product_id", "delta_id").orderBy(col("create_time").asc_nulls_last());
		Dataset<Row> checkParse = check.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank")
				.distinct().coalesce(1);
		checkParse.createOrReplaceTempView("OtaCheckBase");
		beforePartition(spark);
		String sql = "insert overwrite table stats_interface_check_info_base partition(pt='" + pt + "') " +
				"select mid,device_id,product_id,delta_id,origin_version,now_version,create_time,ip," +
				"province,city,networktype,lac,cid,mcc,mnc,rxlev,data_type from OtaCheckBase";
		logger.warn("executing the sql:" + sql);

		spark.sql(sql);
	}

	private void completeMissingData(SparkSession spark, String pt) {
		logger.warn("begin to complete the missing CheckBaseLog");
		WindowSpec w = Window.partitionBy("device_id", "product_id", "version").orderBy(col("create_time").asc_nulls_last());
		String appLogSql = " select * from ota_app_log where pt='" + pt + "' and device_id is not null" +
				" and product_id is not null and mid is not null and version is not null and create_time is not null";
		logger.warn(appLogSql);
		Dataset<Row> appLogBase = spark.sql(appLogSql);
		Dataset<Row> appParse = appLogBase.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank").distinct();
		String endPt= DateUtil.ptAfterOrBeforeDay(pt, -1);
		String beginPt= DateUtil.ptAfterOrBeforeDay(pt, -7);
		String checkBaseSql = " select * from stats_interface_check_info_base where pt >='" + beginPt + "' and pt<='" + endPt + "'";
		logger.warn(checkBaseSql);
		WindowSpec wDesc = Window.partitionBy("device_id", "product_id", "delta_id").orderBy(col("create_time").desc_nulls_last());
		Dataset<Row> filterCheck = spark.sql(checkBaseSql).withColumn("rank",row_number().over(wDesc)).where(col("rank").equalTo(1)).drop("rank").distinct();
		Dataset<Row> result = appParse.join(filterCheck, appParse.col("product_id").equalTo(filterCheck.col("product_id"))
				.and(appParse.col("mid").equalTo(filterCheck.col("mid"))).and(appParse.col("device_id").equalTo(filterCheck.col("device_id"))
				).and(appParse.col("version").equalTo(filterCheck.col("now_version")))).select(appParse.col("mid"), appParse.col("ip"),appParse.col("data_type"),
				appParse.col("version").as("now_version"), appParse.col("device_id"), appParse.col("product_id"),
				appParse.col("province_zh").as("province"), appParse.col("city_zh").as("city"), appParse.col("create_time"), appParse.col("pt")
				, filterCheck.col("origin_version"), filterCheck.col("delta_id")).repartition(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_MISSING_DATA_TMP_PATH);
	}

	public static void main(String[] args) throws IOException {

		String pt = DateUtil.producePtOrYesterday(args);
		OtaCheckBase otaCheckBase = new OtaCheckBase();
		otaCheckBase.runAll(pt);
	}
}
