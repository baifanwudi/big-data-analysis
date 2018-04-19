package com.adups.offline.save.device;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.ProductVersion;
import com.adups.config.PropertiesConfig;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.device.ProductVersionSave;
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
 * @date 02/11/2017.
 */
public class StatsProductVersion extends AbstractSparkSql{

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		String tableName="ota_device_version";

		Dataset<Row> otaDeviceVersion=spark.read().format("jdbc")
				.option("url", PropertiesConfig.URL)
				.option("dbtable",tableName)
				.option("user",PropertiesConfig.USERNAME)
				.option("password",PropertiesConfig.PASSWORD).load();

		ProductVersion productVersion=new ProductVersion();
		@SuppressWarnings("unchecked")
		Dataset<ProductVersion> result=otaDeviceVersion.groupBy("product_id","version").agg(functions.countDistinct("device_id").as("num"))
				.withColumnRenamed("product_id","productId").coalesce(1).as(productVersion.produceBeanEncoder());

		try {
			insertMysql(result);
		} catch (SQLException e) {
			logger.error("insert error"+e.getMessage());
		}
	}

	public void insertMysql(Dataset<ProductVersion> dataSet) throws SQLException {
		String sql = "truncate stats_product_version";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			new ProductVersionSave().putDataBatch(data);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsProductVersion statsProductVersion=new StatsProductVersion();
		statsProductVersion.runAll(pt);
	}

}
