package com.adups.offline.save.base;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.UpgradeBase;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.BeforeBatchPut;
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
public class StatsUpgradeBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		String sql = "select mid,device_id as deviceId, product_id as productId,origin_version as originVersion,now_version as nowVersion," +
				"update_status as updateStatus,delta_id as deltaId,ip,province,apn_type as apnType,city,ext_str as extStr, " +
				"create_time as createTime,data_type as dataType, pt from stats_interface_upgrade_info_base where pt='" + pt + "'";
		logger.warn(" the executing sql is: " + sql);
		UpgradeBase upgradeBase = new UpgradeBase();
		@SuppressWarnings("unchecked")
		Dataset<UpgradeBase> upgradeBaseDataSet = (Dataset<UpgradeBase>) spark.sql(sql).as(upgradeBase.produceBeanEncoder());
		upgradeBaseStats(upgradeBaseDataSet, pt);
		try {
			insertToMysql(upgradeBaseDataSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	private void upgradeBaseStats(Dataset<?> dataSet, String pt) {
		Dataset<Row> result = dataSet.withColumn("countUpgrade", when(col("updateStatus").equalTo(1), 1).otherwise(0))
				.withColumn("countUpgradeFail", when(col("updateStatus").notEqual(1), 1).otherwise(0))
				.groupBy("productId", "deltaId", "originVersion", "nowVersion","dataType").agg(sum("countUpgrade").as("countUpgrade"),
						sum("countUpgradeFail").as("countUpgradeFail")).withColumn("pt", lit(pt)).coalesce(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_UPGRADE_BASE_COUNT_PATH);
	}

	public void insertToMysql(Dataset<UpgradeBase> dataSet, String pt) throws SQLException {
		String sql = "delete from  stats_interface_upgrade_info_base where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into stats_interface_upgrade_info_base set mid=?,device_id=?, product_id =?,origin_version=?,now_version=?," +
					"update_status=?,delta_id =?,ip=?,province=?,apn_type=?,city=?,ext_str=?,create_time=?,data_type=?,pt=?";
			new UpgradeSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsUpgradeBase statsUpgradeBase = new StatsUpgradeBase();
		statsUpgradeBase.runAll(pt);
	}

}
