package com.demo.offline.hive.device;

import com.demo.base.AbstractSparkSql;
import com.demo.config.OnlineOfflinePath;
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
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

/**
 * @author allen
 * Created by allen on 16/08/2017.
 */
public class OffDeviceRegionTotalStats extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		Dataset<Row> product=spark.read().parquet(OnlineOfflinePath.OFFLINE_PRODUCT_ONLINE_PATH).select("product_id");

		String prePath=DateUtil.pathPtWithPre(pt);
		String sql="select product_id,device_id,country_zh as country ,province_zh as province,data_type from device_info where pt='"+pt+"' ";
		logger.warn("executing the sql is:"+sql);
		Dataset<Row> deviceInfo=spark.sql(sql).join(product,"product_id");


		String sqlAll="select product_id,device_id,country_zh as country ,province_zh as province,data_type from device_info where pt<='"+pt+"' ";
		logger.warn("executing the sql is:"+sqlAll);
		Dataset<Row> deviceInfoAll=spark.sql(sqlAll).join(product,"product_id");

		Dataset<Row> deviceNewRegion=deviceInfo.groupBy("product_id","data_type").agg(functions.countDistinct("device_id").as("new_num"));
		Dataset<Row> deviceTotalRegion=deviceInfoAll.groupBy("product_id","data_type").agg(functions.countDistinct("device_id").as("total_num"));

		Seq<String> seq= CommonUtil.columnNames("product_id,data_type");
		Seq<String> naFillZero= CommonUtil.columnNames("new_num,total_num");

		Dataset<Row> regionTotal=deviceNewRegion.join(deviceTotalRegion,seq,"outer").na().fill(0, naFillZero).withColumn("pt",lit(pt)).coalesce(1);
		regionTotal.createOrReplaceTempView("DeviceRegionSplit");
		beforePartition(spark);
		spark.sql("insert overwrite table stats_device_region_split partition(pt='" +pt+ "') select product_id,new_num,total_num,data_type from DeviceRegionSplit");

		Dataset<Row> resultTotal=regionTotal.groupBy("product_id").agg(sum("new_num").as("new_num"),sum("total_num").as("total_num"));
		resultTotal.createOrReplaceTempView("deviceTotal");
		beforePartition(spark);
		spark.sql("insert overwrite table stats_device_total partition(pt='" +pt+ "') select product_id,new_num,total_num from deviceTotal");
		resultTotal.selectExpr("product_id as productId","new_num as newNum","total_num as totalNum").coalesce(1).toDF()
				.write().mode("overwrite").format("parquet").save(OnlineOfflinePath.OFFLINE_DEVICE_NEW_TOTAL_PATH+prePath);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OffDeviceRegionTotalStats offDeviceRegionTotalStats=new OffDeviceRegionTotalStats();
		offDeviceRegionTotalStats.runAll(pt);
	}
}
