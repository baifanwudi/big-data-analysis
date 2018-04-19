package com.adups.test.bak.device;

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

import static org.apache.spark.sql.functions.*;
/**
 * @author allen
 * Created by allen on 01/08/2017.
 */
public class DeviceAreaOffline extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(DeviceAreaOffline.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String prePath=DateUtil.pathPtWithPre(pt);
		String tableName="product";

		Dataset<Row> product=new ReadTable().loadTable(spark,tableName).select("product_id");

		String sql="select product_id,device_id,country_zh as country ,province_zh as province,data_type from ct_iot.device_info where pt='"+pt+"' ";
		logger.warn("executing the sql is:"+sql);
		Dataset<Row> deviceInfo=spark.sql(sql).join(product,"product_id").cache();

		String sqlAll="select product_id,device_id,country_zh as country ,province_zh as province,data_type from ct_iot.device_info where pt<='"+pt+"' ";
		logger.warn("executing the sql is:"+sqlAll);
		Dataset<Row> deviceInfoAll=spark.sql(sqlAll).join(product,"product_id").cache();

		Dataset<Row> deviceArea=deviceInfo.groupBy("product_id","country","province").agg(functions.countDistinct("device_id").as("new_num"));
		Dataset<Row> deviceAreaAll=deviceInfoAll.groupBy("product_id","country","province").agg(functions.countDistinct("device_id").as("total_num"));

		Seq<String> seq= CommonUtil.columnNames("product_id,country,province");
		Seq<String> naFillZero= CommonUtil.columnNames("new_num,total_num");

		Dataset<Row>  resultArea=deviceArea.join(deviceAreaAll,seq,"outer").na().fill(0, naFillZero).withColumn("pt",lit(pt)).repartition(1);

		resultArea.createOrReplaceTempView("deviceArea");

		beforePartition(spark);
		spark.sql("insert overwrite table stats_device_area partition(pt='" + pt+ "') select product_id,country,province,new_num,total_num from deviceArea");
		resultArea.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_DEVICE_AREA_NEW_TOTAL_PATH+prePath);

		Dataset<Row> deviceNewRegion=deviceInfo.groupBy("product_id","data_type").agg(functions.countDistinct("device_id").as("new_num"));
		Dataset<Row> deviceTotalRegion=deviceInfoAll.groupBy("product_id","data_type").agg(functions.countDistinct("device_id").as("total_num"));

		seq= CommonUtil.columnNames("product_id,data_type");
		Dataset<Row> regionTotal=deviceNewRegion.join(deviceTotalRegion,seq,"outer").na().fill(0, naFillZero).withColumn("pt",lit(pt)).repartition(1);
		regionTotal.createOrReplaceTempView("DeviceRegionSplit");
		beforePartition(spark);
		spark.sql("insert overwrite table stats_device_region_split partition(pt='" +pt+ "') select product_id,new_num,total_num,data_type from DeviceRegionSplit");

		Dataset<Row> resultTotal=regionTotal.groupBy("product_id").agg(sum("new_num").as("new_num"),sum("total_num").as("total_num"));
		resultTotal.createOrReplaceTempView("deviceTotal");
		beforePartition(spark);
		spark.sql("insert overwrite table stats_device_total partition(pt='" +pt+ "') select product_id,new_num,total_num from deviceTotal");
		resultTotal.toDF().write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_DEVICE_NEW_TOTAL_PATH+prePath);

		deviceInfo.unpersist();
		deviceInfoAll.unpersist();
	}

	public static void main(String[] args) throws IOException {

		String pt = DateUtil.producePtOrYesterday(args);
		DeviceAreaOffline deviceAreaOffline=new DeviceAreaOffline();
		deviceAreaOffline.runAll(pt);
	}
}
