package com.adups.test.history;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.DownBase;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.sql.logfilter.DownSave;
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
public class HistoryStatsDownBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		String sql = "select * from ct_iot.stats_interface_download_info_base ";
		logger.warn("the executing sql is : " + sql);
		DownBase downBase = new DownBase();

		Dataset<DownBase> downBaseDataSet = (Dataset<DownBase>) spark.sql(sql).as(downBase.produceBeanEncoder());
		downBaseStats(downBaseDataSet, pt);
		try {
			insertToMysql(downBaseDataSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

	}

	public void downBaseStats(Dataset<?> dataSet, String pt) {
		Dataset<Row> result = dataSet.withColumn("count_download", when(col("download_status").equalTo(1), 1).otherwise(0))
				.withColumn("count_downfail", when(col("download_status").notEqual(1), 1).otherwise(0))
				.groupBy("product_id", "delta_id", "origin_version", "now_version","pt").agg(sum("count_download").as("count_download"),
						sum("count_downfail").as("count_downfail")).repartition(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_DOWN_BASE_COUNT_PATH);

	}

	public void insertToMysql(Dataset<DownBase> dataSet, String pt) throws SQLException {
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into stats_interface_download_info_base set mid=?,device_id=?,product_id=?,origin_version=?," +
					"now_version=?,down_start=?,down_end=?,delta_id=?,download_status=?,create_time=?,ip=?,province=?,apn_type=?," +
					"city=?, ext_str=?,down_size=?,down_ip=?,data_type=?,pt=?";
			new DownSave().putDataBatch(data,insertMysql);
		});
	}
	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		HistoryStatsDownBase historyStatsDownBase = new HistoryStatsDownBase();
		historyStatsDownBase.runAll(pt);

	}
}
