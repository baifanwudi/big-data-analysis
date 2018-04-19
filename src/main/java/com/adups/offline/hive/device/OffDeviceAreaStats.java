package com.adups.offline.hive.device;

import com.adups.base.AbstractSparkSql;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.ReadTable;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import java.io.IOException;
import static org.apache.spark.sql.functions.lit;

/**
 * @author allen
 * Created by allen on 16/08/2017.
 */
public class OffDeviceAreaStats  extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		String prePath=DateUtil.pathPtWithPre(pt);

		String tableName="product";
		Dataset<Row> productTable=new ReadTable().loadTable(spark,tableName);

		productTable.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_PRODUCT_ONLINE_PATH);

		Dataset<Row> product=productTable.select("product_id");

		String sql="select product_id,device_id,country_zh as country ,province_zh as province from device_info where pt='"+pt+"' ";
		logger.warn("executing the sql is:"+sql);
		Dataset<Row> deviceInfo=spark.sql(sql).join(product,"product_id");

		String sqlAll="select product_id,device_id,country_zh as country ,province_zh as province from device_info where pt<='"+pt+"' ";
		logger.warn("executing the sql is:"+sqlAll);
		Dataset<Row> deviceInfoAll=spark.sql(sqlAll).join(product,"product_id");

		Dataset<Row> deviceArea=deviceInfo.groupBy("product_id","country","province").agg(functions.countDistinct("device_id").as("new_num"));
		Dataset<Row> deviceAreaAll=deviceInfoAll.groupBy("product_id","country","province").agg(functions.countDistinct("device_id").as("total_num"));

		Seq<String> seq= CommonUtil.columnNames("product_id,country,province");
		Seq<String> naFillZero= CommonUtil.columnNames("new_num,total_num");

		Dataset<Row> resultArea=deviceArea.join(deviceAreaAll,seq,"outer").na().fill(0, naFillZero).withColumn("pt",lit(pt)).repartition(1);

		resultArea.createOrReplaceTempView("deviceArea");
		beforePartition(spark);
		spark.sql("insert overwrite table stats_device_area partition(pt='" + pt+ "') select product_id,country,province,new_num,total_num from deviceArea");
		resultArea.selectExpr("product_id as productId","country","province","new_num as newNum","total_num as totalNum").coalesce(1)
				.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_DEVICE_AREA_NEW_TOTAL_PATH+prePath);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OffDeviceAreaStats offDeviceAreaStats=new OffDeviceAreaStats();
		offDeviceAreaStats.runAll(pt);
	}
}
