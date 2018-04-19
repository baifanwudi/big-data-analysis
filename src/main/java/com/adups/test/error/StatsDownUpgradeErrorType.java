package com.adups.test.error;

import com.adups.base.AbstractSparkSql;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import java.io.IOException;
import static org.apache.spark.sql.functions.count;

/**
 * @author allen
 * @date 16/10/2017.
 */
public class StatsDownUpgradeErrorType extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		String downSql="select * from ct_iot.stats_interface_download_info_base where pt='"+pt+"'";
		logger.warn("the executing sql is : " + downSql);
		Dataset<Row> down=spark.sql(downSql).filter(col("download_status").notEqual("1"))
				.groupBy("product_id","delta_id","origin_version","now_version","download_status")
				.agg(count("device_id").as("down__error_count")).withColumnRenamed("download_status","down_error_type");
		down.show();

		String upgradeSql="select * from ct_iot.stats_interface_upgrade_info_base where  pt='"+pt+"'";
		logger.warn("the executing sql is : " + upgradeSql);
		Dataset<Row> upgrade=spark.sql(upgradeSql).filter(col("update_status").notEqual("1")).groupBy("product_id",
				"delta_id","origin_version","now_version","update_status").agg(count("device_id").as("upgrade_error_count"))
				.withColumnRenamed("update_status","upgrade_error_type");
		upgrade.show();

	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsDownUpgradeErrorType statsDownUpgradeErrorType =new StatsDownUpgradeErrorType();
		statsDownUpgradeErrorType.runAll(pt);
	}
}
