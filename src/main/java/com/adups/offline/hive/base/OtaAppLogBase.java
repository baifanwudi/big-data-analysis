package com.adups.offline.hive.base;

import com.adups.base.AbstractSparkSql;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * @author allen
 * @date 21/12/2017.
 */
public class OtaAppLogBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String productActiveSql = "select product_id ,device_id ,data_type  from ota_app_log where pt='" + pt + "' ";
		logger.warn(productActiveSql);
		Dataset<Row> appLogActiveRegion = spark.sql(productActiveSql).groupBy("product_id","data_type").agg(functions.countDistinct(col("device_id"))
				.as("active_num")).coalesce(1);

		appLogActiveRegion.createOrReplaceTempView("StatsAppLogRegion");
		beforePartition(spark);
		String regionSql ="insert overwrite table stats_app_log_region partition(pt='"+pt+"') select product_id, data_type, active_num from StatsAppLogRegion ";
		logger.warn("executing sql is : "+regionSql);
		spark.sql(regionSql);

		Dataset<Row> appLogActiveTotal=appLogActiveRegion.groupBy("product_id").agg(sum("active_num").as("active_num")).coalesce(1);
		appLogActiveTotal.createOrReplaceTempView("StatsAppLogTotal");

		String totalSql="insert overwrite table stats_app_log_total partition(pt='"+pt+"') select product_id,active_num from StatsAppLogTotal";
		logger.warn("executing sql is : "+totalSql);
		spark.sql(totalSql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaAppLogBase otaAppLogBase =new OtaAppLogBase();
		otaAppLogBase.runAll(pt);
	}
}
