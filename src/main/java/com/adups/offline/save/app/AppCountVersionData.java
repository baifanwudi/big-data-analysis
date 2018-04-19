package com.adups.offline.save.app;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.AppCountVersion;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.app.AppCountVersionSave;
import com.adups.config.PropertiesConfig;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountVersionData extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String tableName="app_version_new";
		Dataset<Row> appCountVersionData=spark.read().format("jdbc")
				.option("url", PropertiesConfig.URL)
				.option("dbtable",tableName)
				.option("user",PropertiesConfig.USERNAME)
				.option("password",PropertiesConfig.PASSWORD).load();

		AppCountVersion appCountVersion=new AppCountVersion();
		@SuppressWarnings("unchecked")
		Dataset<AppCountVersion> result=appCountVersionData.groupBy("product_id","package_name","version_name").agg(
				functions.countDistinct("device_id").as("total_num")).withColumnRenamed("product_id","productId")
				.withColumnRenamed("package_name","packageName").withColumnRenamed("version_name","packageVersion")
				.withColumnRenamed("total_num","totalNum").coalesce(1).as(appCountVersion.produceBeanEncoder());
		try {
			insertMysql(result);
		} catch (SQLException e) {
			logger.error("insert error"+e.getMessage());
		}
	}

	public void insertMysql(Dataset<AppCountVersion> dataSet) throws SQLException {
		String sql = "truncate app_count_version_data";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into app_count_version_data set product_id=?,package_name=?,package_version=?,total_num=?";
			new AppCountVersionSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		AppCountVersionData appCountVersionData =new AppCountVersionData();
		appCountVersionData.runAll(pt);
	}
}
