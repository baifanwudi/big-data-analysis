package com.demo.offline.save.stats;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.out.ProductTotalMerge;
import com.demo.common.BeforeBatchPut;
import com.demo.common.sql.stats.ProductTotalMergeSave;
import com.demo.util.CommonUtil;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 *         Created by allen on 09/08/2017.
 */
public class StatsProductTotalMerge extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {

		String sql = "select product_id as productId,new_num as newNum, total_num as totalNum, pt from stats_device_total where pt='" + pt + "'";
		logger.warn(sql);
		Dataset<Row> deviceTotal = spark.sql(sql);

		String offlineSql = "select product_id as productId,delta_id as deltaId,origin_version as originVersion,target_version as targetVersion," +
				"count_check as countCheck,count_download as countDownload,count_upgrade as countUpgrade,count_downfail as countDownFail," +
				"count_upgradefail as countUpgradeFail,pt from stats_product_delta_merge  where pt='" + pt + "'";
		logger.warn(offlineSql);
		Dataset<Row> offlineMergeStats = spark.sql(offlineSql).groupBy("productId").agg(
				sum(col("countCheck")).as("countCheck"), sum(col("countDownload")).as("countDownload"),
				sum(col("countUpgrade")).as("countUpgrade"), sum(col("countDownFail")).as("countDownFail")
				, sum(col("countUpgradeFail")).as("countUpgradeFail")).withColumn("pt", lit(pt));

		String activeSql="select product_id as productId,active_num as activeNum, pt from stats_app_log_total where pt='" + pt + "' ";
		logger.warn(activeSql);
		Dataset<Row> appLogActive = spark.sql(activeSql);

		Seq<String> seq = CommonUtil.columnNames("productId,pt");
		ProductTotalMerge productTotalMerge = new ProductTotalMerge();

		@SuppressWarnings("unchecked")
		Dataset<ProductTotalMerge> result=(Dataset<ProductTotalMerge> )deviceTotal.join(offlineMergeStats,seq,"outer").join(appLogActive,seq,"outer")
				.na().fill(0).repartition(2).as(productTotalMerge.produceBeanEncoder());
		try {
			insertToMysql(result, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertToMysql(Dataset<ProductTotalMerge> dataset, String pt) throws SQLException {
		String sql = "delete from  stats_product_total_merge where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataset.foreachPartition(data -> {
			String insertMysql = "insert into stats_product_total_merge set product_id=?,active_num=?,new_num=?," +
					"total_num=?,count_check=?,count_download=?,count_upgrade=?,count_downfail=?,count_upgradefail=?,pt=? ";
			new ProductTotalMergeSave().putDataBatch(data, insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsProductTotalMerge statsProductTotalMerge = new StatsProductTotalMerge();
		statsProductTotalMerge.runAll(pt);
	}
}
