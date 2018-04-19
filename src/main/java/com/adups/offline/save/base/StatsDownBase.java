package com.adups.offline.save.base;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.DownBase;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.logfilter.DownSave;
import com.adups.util.DateUtil;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.SQLException;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.col;

/**
 * @author allen
 * Created by allen on 07/08/2017.
 */
public class StatsDownBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		String sql = "select mid,device_id as deviceId,product_id as productId ,origin_version as originVersion," +
				"now_version as nowVersion ,down_start as downStart,down_end as downEnd,delta_id as deltaId," +
				"download_status as downloadStatus,create_time as createTime,ip,province,apn_type as apnType, city, " +
				"ext_str as extStr,down_size as downSize,down_ip as downIp,data_type as dataType, pt from " +
				"stats_interface_download_info_base where pt='" + pt + "'";
		logger.warn("the executing sql is : " + sql);
		DownBase downBase = new DownBase();
		@SuppressWarnings("unchecked")
		Dataset<DownBase> downBaseDataSet = (Dataset<DownBase>) spark.sql(sql).as(downBase.produceBeanEncoder());
		downBaseStats(downBaseDataSet, pt);
		try {
			insertToMysql(downBaseDataSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	private void downBaseStats(Dataset<?> dataSet, String pt) {
		Dataset<Row> result = dataSet.withColumn("countDownload", when(col("downloadStatus").equalTo(1), 1).otherwise(0))
				.withColumn("countDownFail", when(col("downloadStatus").notEqual(1), 1).otherwise(0))
				.groupBy("productId", "deltaId", "originVersion", "nowVersion","dataType").agg(sum("countDownload").as("countDownload"),
						sum("countDownFail").as("countDownFail")).withColumn("pt", lit(pt)).coalesce(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_DOWN_BASE_COUNT_PATH);
	}

	public void insertToMysql(Dataset<DownBase> dataSet, String pt) throws SQLException {
		String sql = "delete from  stats_interface_download_info_base where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
		String insertMysql = "insert into stats_interface_download_info_base set mid=?,device_id=?,product_id=?,origin_version=?," +
				"now_version=?,down_start=?,down_end=?,delta_id=?,download_status=?,create_time=?,ip=?,province=?,apn_type=?," +
				"city=?, ext_str=?,down_size=?,down_ip=?,data_type=?,pt=?";
			new DownSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsDownBase statsDownBase = new StatsDownBase();
		statsDownBase.runAll(pt);
	}
}
