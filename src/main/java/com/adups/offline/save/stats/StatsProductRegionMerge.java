package com.adups.offline.save.stats;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.ProductRegionMerge;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.stats.ProductRegionMergeSave;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
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
public class StatsProductRegionMerge extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {

		String sql = "select product_id as productId,new_num as newNum ,total_num as totalNum,data_type as dataType, pt" +
				" from stats_device_region_split where pt='" + pt + "'";
		logger.warn(sql);
		Dataset<Row> deviceTotal = spark.sql(sql);

		String offlineSql = "select product_id as productId,delta_id as deltaId,origin_version as originVersion," +
				"target_version as targetVersion,count_check as countCheck,count_download as countDownload,count_upgrade as countUpgrade," +
				"count_downfail as countDownFail,count_upgradefail as countUpgradeFail,data_type as dataType,pt" +
				" from stats_product_delta_region_split where pt='" + pt + "'";
		logger.warn(offlineSql);
		Dataset<Row> offlineMergeStats = spark.sql(offlineSql).groupBy("productId","dataType").agg(
				sum(col("countCheck")).as("countCheck"), sum(col("countDownload")).as("countDownload"),
				sum(col("countUpgrade")).as("countUpgrade"), sum(col("countDownFail")).as("countDownFail")
				, sum(col("countUpgradeFail")).as("countUpgradeFail")).withColumn("pt", lit(pt));
		String activeSql="select product_id as productId,data_type as dataType,active_num as activeNum,pt from stats_app_log_region where pt='" + pt + "' ";
		logger.warn(activeSql);
		Dataset<Row> appLogActive=spark.sql(activeSql);
		Seq<String> seq = CommonUtil.columnNames("productId,pt,dataType");
		ProductRegionMerge productRegionMerge = new ProductRegionMerge();

		@SuppressWarnings("unchecked")
		Dataset<ProductRegionMerge> result=deviceTotal.join(offlineMergeStats,seq,"outer").join(appLogActive,seq,"outer")
				.na().fill(0).repartition(2).as(productRegionMerge.produceBeanEncoder());
		try {
			insertToMysql(result, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertToMysql(Dataset<ProductRegionMerge> dataSet, String pt) throws SQLException {
		String sql = "delete from  stats_product_region_merge where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into stats_product_region_merge set product_id=?,active_num=?,new_num=?," +
					"total_num=?,count_check=?,count_download=?,count_upgrade=?,count_downfail=?,count_upgradefail=?,data_type=?,pt=? ";
			new ProductRegionMergeSave().putDataBatch(data, insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsProductRegionMerge statsProductTotalMerge = new StatsProductRegionMerge();
		statsProductTotalMerge.runAll(pt);
	}
}
