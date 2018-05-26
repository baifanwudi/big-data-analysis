package com.demo.offline.hive.base;

import com.demo.config.OnlineOfflinePath;
import com.demo.util.CommonUtil;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 02/08/2017.
 */
public class OtaUpgradeBase extends AbstractTimeFilter {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		parseUpgradeLog(spark, pt);
		completeMissingData(spark, pt);
	}

	/**
	 * 清洗ota_interface_upgrade_info_log表到stats_interface_upgrade_info_base
	 */
	private void parseUpgradeLog(SparkSession spark, String pt) {

		String ptWeek = DateUtil.ptAfterOrBeforeDay(pt, -7);
		Dataset<Row> sqlResult=spark.sql("select * from ota_interface_upgrade_info_log where pt>= '" + ptWeek + "' and pt<='" + pt + "' and create_time is not null and " +
				" product_id is not null and delta_id is not null and origin_version is not null");

		WindowSpec w = Window.partitionBy("device_id", "product_id", "delta_id", "update_status").orderBy(col("create_time").asc_nulls_last());
		Dataset<Row> upgrade = sqlResult.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank");

		Dataset<Row> upgradeParse=filterTimeIsNotPt(upgrade,true,pt).distinct().coalesce(1);
		if (upgradeParse.count() == 0) {
			logger.error("the count of ota_interface_upgrade_info_log in " + pt + " is 0 ");
			return;
		}
		upgradeParse.createOrReplaceTempView("OtaUpgradeBase");
		beforePartition(spark);
		String sql = " insert overwrite table stats_interface_upgrade_info_base partition(pt='" + pt + "') select " +
				"mid,device_id,product_id,origin_version,now_version,update_status,delta_id,ip,province,apn_type," +
				"city,ext_str,create_time,data_type from OtaUpgradeBase";
		logger.warn(sql);
		spark.sql(sql);
	}

	/**
	 * 	补数据
	 */
	private void completeMissingData(SparkSession spark, String pt) {

		String ptWeek = DateUtil.ptAfterOrBeforeDay(pt, -7);
//		complementUpgradeWithoutDownload(spark,pt,ptWeek);
		complementCheckWithoutUpgrade(spark,pt,ptWeek);
	}

	private void complementUpgradeWithoutDownload(SparkSession spark, String pt,String ptWeek){

		Seq<String> seq = CommonUtil.columnNames("mid,device_id,product_id,delta_id,origin_version,now_version");
		//如果一个项目当天升级成功,但七天内找不到下载成功记录,就补这条下载成功记录
		logger.warn(" begin to complete the upgrade success without download success");
		Dataset<Row> download=spark.sql("select * from stats_interface_download_info_base where download_status='1' and pt>='" + ptWeek + "' and pt<='" + pt + "'");
		Dataset<Row> upgrade=spark.sql("select * from stats_interface_upgrade_info_base  where pt='"+pt+"' and update_status='1' " );
		Dataset<Row> missDownload=upgrade.join(download,seq, "left").filter(upgrade.col("pt").isNull()).
				select(upgrade.col("*")).repartition(1);
		if (missDownload.count() == 0) {
			logger.error("the count of the upgrade  success without download  message in " + pt + " is 0 ");
			return;
		}
		missDownload.createOrReplaceTempView("MissDownload");
		beforePartition(spark);
		String downMissSql = "insert into table stats_interface_download_info_base partition(pt='" + pt + "') select " +
				"mid,device_id,product_id,origin_version,now_version,null,null,delta_id,1,create_time,ip,province,0," +
				"city,'only upgrade missing lost',null,null,data_type from MissDownload";
		logger.warn(downMissSql);
		spark.sql(downMissSql);
	}

	private void complementCheckWithoutUpgrade(SparkSession spark, String pt,String ptWeek){

		Seq<String> seq = CommonUtil.columnNames("mid,device_id,product_id,delta_id,origin_version,now_version");
		//根据登录日志,补升级成功记录
		logger.warn(" begin to complete the missing UpgradeBaseLog");
		Dataset<Row> filterCheckBase = spark.read().parquet(OnlineOfflinePath.OFFLINE_MISSING_DATA_TMP_PATH);
		if (filterCheckBase.count() == 0) {
			logger.error("the count of CompleteMissingData in " + pt + " is 0 ");
			return;
		}
		Dataset<Row> filterUpgrade = spark.sql("select * from stats_interface_upgrade_info_base where pt>='" + ptWeek + "' and pt<='" + pt + "' and update_status='1' " );
		Dataset<Row> result = filterCheckBase.join(filterUpgrade, seq, "left").filter(filterUpgrade.col("pt").isNull()).
				select(filterCheckBase.col("*")).coalesce(1);
		result.createOrReplaceTempView("UpgradeComplement");
		beforePartition(spark);
		String sql = "insert into table stats_interface_upgrade_info_base partition(pt='" + pt + "') select " +
				"mid,device_id,product_id,origin_version,now_version,1,delta_id,ip,province,0," +
				"city,'lost',create_time,data_type from UpgradeComplement";
		logger.warn(sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaUpgradeBase otaDownloadBase = new OtaUpgradeBase();
		otaDownloadBase.runAll(pt);
	}
}
