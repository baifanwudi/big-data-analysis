package com.adups.test.history;

import com.adups.base.AbstractSparkSql;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

/**
 * @author allen
 * Created by allen on 02/08/2017.
 */
public class HistoryOtaCheckBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(HistoryOtaCheckBase.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		parseOtaCheckLog(spark, pt);
	}

	public void parseOtaCheckLog(SparkSession spark, String pt) {

		Dataset<Row> check = spark.sql("select * from ota_interface_check_info_log where  create_time is not null and" +
				" product_id is not null and now_version is not null and origin_version is not null");
		if (check.count() == 0) {
			logger.error("the count of ota_interface_check_info_log in " + pt + " is 0 ");
			return;
		}
		WindowSpec w = Window.partitionBy("device_id", "product_id", "delta_id","pt").orderBy(col("create_time").asc_nulls_last());
		Dataset<Row> checkParse = check.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank")
				.distinct().repartition(1);
		checkParse.createOrReplaceTempView("OtaCheckBase");
		beforePartition(spark);
		String sql = "insert overwrite table stats_interface_check_info_base partition(pt) " +
				"select mid,device_id,product_id,delta_id,origin_version,now_version,create_time,ip," +
				"province,city,networktype,lac,cid,mcc,mnc,rxlev,data_type,pt from OtaCheckBase";
		logger.warn("executing the sql:" + sql);

		spark.sql(sql);

	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		HistoryOtaCheckBase historyOtaCheckBase = new HistoryOtaCheckBase();
		historyOtaCheckBase.runAll(pt);
	}
}
