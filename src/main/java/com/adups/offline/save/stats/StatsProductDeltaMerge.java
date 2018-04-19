package com.adups.offline.save.stats;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.ProductDeltaMerge;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.stats.ProductDeltaMergeSave;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import static  org.apache.spark.sql.functions.col;
import java.io.IOException;
import java.sql.SQLException;
import static org.apache.spark.sql.functions.sum;

/**
 * @author allen
 * Created by allen on 07/08/2017.
 */
public class StatsProductDeltaMerge extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {

		String checkPath= OnlineOfflinePath.OFFLINE_CHECK_BASE_COUNT_PATH;
		String downPath=OnlineOfflinePath.OFFLINE_DOWN_BASE_COUNT_PATH;
		String upgradePath=OnlineOfflinePath.OFFLINE_UPGRADE_BASE_COUNT_PATH;

		Dataset<Row> checkStats=spark.read().parquet(checkPath).filter(col("pt").equalTo(pt));
		Dataset<Row> downStats=spark.read().parquet(downPath).filter(col("pt").equalTo(pt));
		Dataset<Row> upgradeStats=spark.read().parquet(upgradePath).filter(col("pt").equalTo(pt));

		Seq<String> seq= CommonUtil.columnNames("productId,deltaId,originVersion,nowVersion,dataType,pt");
		String[] fillZeroColumns="countCheck,countDownload,countUpgrade,countDownFail,countUpgradeFail".split(",");


		Dataset<Row> deltaSplit=checkStats.join(downStats,seq,"outer").join(upgradeStats,seq,"outer")
				.withColumnRenamed("nowVersion","targetVersion").na().fill(0,fillZeroColumns)
				.na().drop().repartition(1);

		deltaSplit.createOrReplaceTempView("ProductDeltaRegionSplit");

		beforePartition(spark);
		String splitSql = "insert overwrite table stats_product_delta_region_split partition(pt='"+pt+"') " +
				"select productId,deltaId,originVersion,targetVersion,countCheck,countDownload," +
				"countUpgrade,countDownFail,countUpgradeFail,dataType from ProductDeltaRegionSplit";
		logger.warn(" the executing sql is :" + splitSql);
		spark.sql(splitSql);

		ProductDeltaMerge productDeltaMerge =new ProductDeltaMerge();

		@SuppressWarnings("unchecked")
		Dataset<ProductDeltaMerge> result=deltaSplit.groupBy("productId","deltaId","originVersion","targetVersion","pt").
				agg(sum("countCheck").as("countCheck"),sum("countDownload").as("countDownload")
						,sum("countUpgrade").as("countUpgrade"),sum("countDownFail").as("countDownFail")
						,sum("countUpgradeFail").as("countUpgradeFail")).as(productDeltaMerge.produceBeanEncoder());;
		result.createOrReplaceTempView("ProductDeltaMerge");
		beforePartition(spark);
		String mergeSql = "insert overwrite table stats_product_delta_merge partition(pt='"+pt+"') " +
				"select productId,deltaId,originVersion,targetVersion,countCheck,countDownload," +
				"countUpgrade,countDownFail,countUpgradeFail from ProductDeltaMerge";
		logger.warn(" the executing sql is :" + mergeSql);
		spark.sql(mergeSql);

		try {
			insertMysql(result,pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertMysql(Dataset<ProductDeltaMerge> dataSet,String pt) throws SQLException {
		String sql = "delete from stats_product_delta_merge where pt='"+pt+"'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.repartition(3).foreachPartition(data -> {
			new ProductDeltaMergeSave().putDataBatch(data);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsProductDeltaMerge statsProductDeltaMerge =new StatsProductDeltaMerge();
		statsProductDeltaMerge.runAll(pt);
	}
}
