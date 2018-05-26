package com.demo.offline.save.base;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.out.CheckBase;
import com.demo.config.OnlineOfflinePath;
import com.demo.common.BeforeBatchPut;
import com.demo.common.sql.logfilter.CheckSave;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.SQLException;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.count;
/**
 * @author allen
 * Created by allen on 07/08/2017.
 */
public class StatsCheckBase extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		String sql = "select mid,device_id as deviceId,product_id as productId,delta_id as deltaId,origin_version as originVersion," +
				"now_version as nowVersion,create_time as createTime ,ip,province,city,networkType,lac,cid,mcc,mnc,rxlev,data_type as dataType," +
				"pt from stats_interface_check_info_base where pt='" + pt + "'";
		logger.warn("the executing sql is : " + sql);
		CheckBase checkBase = new CheckBase();
		@SuppressWarnings("unchecked")
		Dataset<CheckBase> checkBaseDataSet = (Dataset<CheckBase>) spark.sql(sql).as(checkBase.produceBeanEncoder());
		checkBaseStats(checkBaseDataSet,pt);
		try {
			insertToMysql(checkBaseDataSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	private void checkBaseStats(Dataset<?> dataSet,String pt){
		Dataset<Row> result=dataSet.groupBy("productId","deltaId","originVersion","nowVersion","dataType")
				.agg(count(col("deviceId")).as("countCheck")).withColumn("pt",lit(pt)).coalesce(1);
		result.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_CHECK_BASE_COUNT_PATH);
	}

	public void insertToMysql(Dataset<CheckBase> dataSet, String pt) throws SQLException {
		String sql = "delete from  stats_interface_check_info_base where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into stats_interface_check_info_base set mid=?,device_id=?, product_id =?,delta_id =?,origin_version=?,now_version=?," +
					"create_time=?,ip=?,province=?,city=?,networkType=?,lac=?,cid=?,mcc=?,mnc=?,rxlev=?,data_type=?,pt=?";
			new CheckSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsCheckBase statsCheckBase = new StatsCheckBase();
		statsCheckBase.runAll(pt);
	}

}
