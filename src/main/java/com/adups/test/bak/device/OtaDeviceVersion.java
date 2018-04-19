package com.adups.test.bak.device;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.DeviceVersion;
import com.adups.common.ReadTable;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 27/07/2017.
 */
public class OtaDeviceVersion extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String tableName = "product";

		Dataset<Row> updateInfo = spark.sql("select mid,device_id,product_id,origin_version,now_version,delta_id from " +
				"stats_interface_upgrade_info_base where update_status=1").distinct();

		WindowSpec w = Window.partitionBy("mid", "device_id", "product_id").orderBy(col("delta_id").desc_nulls_last());
		Dataset<Row> maxDeltaMoreInfo = updateInfo.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank").distinct().repartition(1);

		Dataset<Row> deviceInfo = spark.sql("select product_id,mid,device_id,version from device_info").distinct();

		Dataset<Row> product = new ReadTable().loadTable(spark, tableName).select("product_id").distinct();

		Dataset<DeviceVersion> versionBase = maxDeltaMoreInfo.join(deviceInfo, CommonUtil.columnNames("product_id,device_id,mid"), "outer")
				.withColumn("newVersion", when(col("now_version").isNotNull(), col("now_version")).otherwise(col("version")))
				.select("product_id", "device_id", "mid", "newVersion").withColumnRenamed("newVersion", "version").join(product, "product_id")
				.distinct()
				.as(new DeviceVersion().produceBeanEncoder());

		versionBase.createOrReplaceTempView("otaDeviceVersion");
		beforePartition(spark);
		String sql = "insert overwrite table ota_device_version  partition(pt='" + pt + "') select  product_id,device_id,mid,version from  otaDeviceVersion";
		logger.warn("executing the sql is  " + sql);
		spark.sql(sql);
	}
	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaDeviceVersion deviceNewestVersion = new OtaDeviceVersion();
		deviceNewestVersion.runAll(pt);
	}
}
