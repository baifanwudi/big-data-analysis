package com.adups.test.history;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.CheckBase;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.sql.logfilter.CheckSave;
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
public class HistoryStatsCheckBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		String sql = "select * from stats_interface_check_info_base ";
		logger.warn("the executing sql is : " + sql);
		CheckBase checkBase = new CheckBase();
		Dataset<CheckBase> checkBaseDataSet = (Dataset<CheckBase>) spark.sql(sql).as(checkBase.produceBeanEncoder());
		checkBaseStats(checkBaseDataSet,pt);
		try {
			insertToMysql(checkBaseDataSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void checkBaseStats(Dataset<?> dataSet,String pt){
		Dataset<Row> result=dataSet.groupBy("product_id","delta_id","origin_version","now_version","pt")
				.agg(count(col("device_id")).as("count_check")).repartition(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_CHECK_BASE_COUNT_PATH);
	}

	public void insertToMysql(Dataset<CheckBase> dataSet, String pt) throws SQLException {
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into stats_interface_check_info_base set mid=?,device_id=?, product_id =?,delta_id =?,origin_version=?,now_version=?," +
					"create_time=?,ip=?,province=?,city=?,networkType=?,lac=?,cid=?,mcc=?,mnc=?,rxlev=?,data_type=?,pt=?";
			new CheckSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		HistoryStatsCheckBase historyStatsCheckBase = new HistoryStatsCheckBase();
		historyStatsCheckBase.runAll(pt);
	}

}
