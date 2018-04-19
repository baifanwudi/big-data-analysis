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
public class HistoryOtaUpgradeBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		parseUpgradeLog(spark, pt);
	}

	/**
	 *清洗ota_interface_upgrade_info_log表到stats_interface_upgrade_info_base
	 */
	public void parseUpgradeLog(SparkSession spark, String pt) {

		Dataset<Row> upgrade = spark.sql("select * from ota_interface_upgrade_info_log where create_time is not null and " +
				" product_id is not null and delta_id is not null and origin_version is not null  ");

		if (upgrade.count() == 0) {
			logger.error("the count of ota_interface_upgrade_info_log in " + pt + " is 0 ");
			return;
		}

		WindowSpec w = Window.partitionBy("device_id", "product_id", "delta_id", "update_status","pt").orderBy(col("create_time").asc_nulls_last());
		Dataset<Row> upgradeParse = upgrade.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank").distinct().repartition(1);

		upgradeParse.createOrReplaceTempView("OtaUpgradeBase");
		beforePartition(spark);
		String sql = " insert overwrite table stats_interface_upgrade_info_base partition(pt) select " +
				"mid,device_id,product_id,origin_version,now_version,update_status,delta_id,ip,province,apn_type," +
				"city,ext_str,create_time,data_type,pt from OtaUpgradeBase";
		logger.warn(sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		HistoryOtaUpgradeBase historyOtaUpgradeBase = new HistoryOtaUpgradeBase();
		historyOtaUpgradeBase.runAll(pt);
	}
}
