package com.adups.test;

import com.adups.base.AbstractSparkSql;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 04/08/2017.
 */
public class OtaAppLogStats extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(OtaAppLogStats.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String sql="select * from ct_iot.ota_app_log where pt='"+pt+"' ";
		logger.warn(sql);
		Dataset<Row> otaAppLog=spark.sql(sql);

		Dataset<Row> result=otaAppLog.groupBy("product_id").agg(functions.countDistinct(col("device_id")).as ("active_num")).withColumn("pt",lit(pt));

		result.toDF().write().mode("overwrite").format("parquet").save("ota/offline/base/appLogActiveNUm");

	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaAppLogStats otbAppLogStats=new OtaAppLogStats();
		otbAppLogStats.runAll(pt);
	}
}
