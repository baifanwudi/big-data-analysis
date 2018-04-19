package com.adups.test.history;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.UpgradeBase;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.sql.logfilter.UpgradeSave;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 07/08/2017.
 */
public class HistoryStatsUpgradeBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		String sql = "select * from stats_interface_upgrade_info_base ";
		logger.warn(" the executing sql is: " + sql);
		UpgradeBase upgradeBase = new UpgradeBase();
		Dataset<UpgradeBase> upgradeBaseDataSet = (Dataset<UpgradeBase>) spark.sql(sql).as(upgradeBase.produceBeanEncoder());
		upgradeBaseStats(upgradeBaseDataSet, pt);
		try {
			insertToMysql(upgradeBaseDataSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void upgradeBaseStats(Dataset<?> dataSet, String pt) {
		Dataset<Row> result = dataSet.withColumn("count_upgrade", when(col("update_status").equalTo(1), 1).otherwise(0))
				.withColumn("count_upgradefail", when(col("update_status").notEqual(1), 1).otherwise(0))
				.groupBy("product_id", "delta_id", "origin_version", "now_version","pt").agg(sum("count_upgrade").as("count_upgrade"),
						sum("count_upgradefail").as("count_upgradefail")).repartition(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_UPGRADE_BASE_COUNT_PATH);
	}

	public void insertToMysql(Dataset<UpgradeBase> dataset, String pt) throws SQLException {
		dataset.foreachPartition(data -> {
			String insertMysql = "insert into stats_interface_upgrade_info_base set mid=?,device_id=?, product_id =?,origin_version=?,now_version=?," +
					"update_status=?,delta_id =?,ip=?,province=?,apn_type=?,city=?,ext_str=?,create_time=?,data_type=?,pt=?";
			new UpgradeSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		HistoryStatsUpgradeBase historyStatsUpgradeBase = new HistoryStatsUpgradeBase();
		historyStatsUpgradeBase.runAll(pt);
	}

}
