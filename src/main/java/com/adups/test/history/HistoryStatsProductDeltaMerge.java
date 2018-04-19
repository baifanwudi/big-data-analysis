package com.adups.test.history;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.ProductDeltaMerge;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.sql.stats.ProductDeltaMergeSave;
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

/**
 * @author allen
 * Created by allen on 07/08/2017.
 */
public class HistoryStatsProductDeltaMerge extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {

		String checkPath= OnlineOfflinePath.OFFLINE_CHECK_BASE_COUNT_PATH;
		String downPath=OnlineOfflinePath.OFFLINE_DOWN_BASE_COUNT_PATH;
		String upgradePath=OnlineOfflinePath.OFFLINE_UPGRADE_BASE_COUNT_PATH;

		Dataset<Row> checkStats=spark.read().parquet(checkPath);
		Dataset<Row> downStats=spark.read().parquet(downPath);
		Dataset<Row> upgradeStats=spark.read().parquet(upgradePath);

		Seq<String> seq= CommonUtil.columnNames("product_id,delta_id,origin_version,now_version,pt");
		String[] fillZeroColumns="count_check,count_download,count_upgrade,count_downfail,count_upgradefail".split(",");

		ProductDeltaMerge productDeltaMerge =new ProductDeltaMerge();
		Dataset<ProductDeltaMerge> result=checkStats.join(downStats,seq,"outer").join(upgradeStats,seq,"outer")
				.withColumnRenamed("now_version","target_version").na().fill(0,fillZeroColumns)
				.na().drop().repartition(1).as(productDeltaMerge.produceBeanEncoder());
		result.createOrReplaceTempView("ProductDeltaMerge");
		beforePartition(spark);
		String sql = "insert overwrite table stats_product_delta_merge partition(pt) " +
				"select product_id,delta_id,origin_version,target_version,count_check,count_download," +
				"count_upgrade,count_downfail,count_upgradefail,pt from ProductDeltaMerge";
		logger.warn(" the executing sql is :" + sql);
		spark.sql(sql);

		try {
			insertMysql(result,pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

	}

	public void insertMysql(Dataset<ProductDeltaMerge> dataSet,String pt) throws SQLException {
		dataSet.repartition(3).foreachPartition(data -> {
			new ProductDeltaMergeSave().putDataBatch(data);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		HistoryStatsProductDeltaMerge historyStatsProductDeltaMerge =new HistoryStatsProductDeltaMerge();
		historyStatsProductDeltaMerge.runAll(pt);
	}
}
