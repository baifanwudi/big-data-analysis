package com.demo.online.flume;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.out.ProductTotalMerge;
import com.demo.config.FlumePath;
import com.demo.config.OnlineOfflinePath;
import com.demo.common.sql.flume.ProductTotalMergeOnlineSave;
import com.demo.util.CommonUtil;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import java.io.IOException;
import java.sql.SQLException;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 09/08/2017.
 */
public class ProductTotalMergeOnline extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String nowPt = DateUtil.nowPtDay();

		String appLogPath = FlumePath.APP_LOG_PATH + DateUtil.pathNowPtWithPre();
		String deviceNewTotalPath = OnlineOfflinePath.ONLINE_DEVICE_NEW_TOTAL_PATH;
		String productDeltaPath = OnlineOfflinePath.ONLINE_PRODUCT_DELTA_MERGE;
		String originPath = OnlineOfflinePath.ONLINE_PRODUCT_TOTAL_MERGE;

		Dataset<Row> deviceTotal = spark.read().parquet(deviceNewTotalPath).filter(col("pt").equalTo(nowPt));
		Dataset<Row> offlineMergeStats = spark.read().parquet(productDeltaPath).filter(col("pt").equalTo(nowPt)).groupBy("productId").agg(
				sum(col("countCheck")).as("countCheck"), sum(col("countDownload")).as("countDownload"),
				sum(col("countUpgrade")).as("countUpgrade"), sum(col("countDownFail")).as("countDownFail")
				, sum(col("countUpgradeFail")).as("countUpgradeFail")).withColumn("pt", lit(nowPt));

		Seq<String> seq = CommonUtil.columnNames("productId,pt");

		Dataset<Row> result = deviceTotal.join(offlineMergeStats, seq, "outer");
		if (existsPath(appLogPath)) {
			Dataset<Row> appLogActive = spark.read().json(appLogPath).groupBy("productId").agg(functions.countDistinct(col("deviceId"))
					.as("activeNum")).withColumn("pt", lit(nowPt));

			result = result.join(appLogActive, seq, "outer").na().fill(0).coalesce(1);
		} else {
			result = result.withColumn("activeNum", lit(0)).na().fill(0).coalesce(1);
		}

		ProductTotalMerge productTotalMerge = new ProductTotalMerge();
		Dataset<ProductTotalMerge> deltaResult;
		if (existsPath(originPath)) {
			try {
				Dataset<Row> baseProductTotal = spark.read().parquet(originPath);
				deltaResult = result.except(baseProductTotal).as(productTotalMerge.produceBeanEncoder()).coalesce(1);
			}catch (Exception e){
				logger.error(e.getMessage());
				//特殊情况,文件路径存在,文件内容丢失
				deltaResult = result.as(productTotalMerge.produceBeanEncoder()).coalesce(1);
			}
		} else {
			deltaResult = result.as(productTotalMerge.produceBeanEncoder()).coalesce(1);
		}

		try {
			insertToMysql(deltaResult);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		result.write().mode("overwrite").format("parquet").save(originPath);
	}

	public void insertToMysql(Dataset<ProductTotalMerge> dataSet) throws SQLException {

		dataSet.foreachPartition(data -> {
			String sql = "insert into stats_product_total_merge(product_id,active_num,new_num,total_num," +
					"count_check,count_download,count_upgrade,count_downfail,count_upgradefail,pt) values(?,?,?,?," +
					"?,?,?,?,?,?)on duplicate key update active_num=?, new_num=?,total_num=?,count_check=?,count_download=?," +
					"count_upgrade=?,count_downfail=?,count_upgradefail=?";
			new ProductTotalMergeOnlineSave().putDataBatch(data, sql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		ProductTotalMergeOnline productTotalMergeOnline = new ProductTotalMergeOnline();
		productTotalMergeOnline.runAll(pt, false);
	}
}
