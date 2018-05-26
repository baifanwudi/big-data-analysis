package com.demo.online.flume;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.out.DeviceArea;
import com.demo.config.OnlineOfflinePath;
import com.demo.common.ReadTable;
import com.demo.common.sql.flume.DeviceAreaOnlineSave;
import com.demo.util.CommonUtil;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import static org.apache.spark.sql.functions.*;
import java.io.IOException;

/**
 * @author allen
 * Created by allen on 03/08/2017.
 */
public class DeviceAreaOnline extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		String prePath = DateUtil.pathPtWithPre(pt);
		String nowPt = DateUtil.nowPtDay();

		String beginTime = nowPt + " 00:00:00";
		String endTime = nowPt + " 23:59:59";

		String deviceTotal = OnlineOfflinePath.OFFLINE_DEVICE_NEW_TOTAL_PATH + prePath;
		String deviceAreaTotal = OnlineOfflinePath.OFFLINE_DEVICE_AREA_NEW_TOTAL_PATH + prePath;
		String originAreaPath = OnlineOfflinePath.ONLINE_DEVICE_AREA_NEW_TOTAL_PATH;

		if (!existsPath(deviceAreaTotal, deviceAreaTotal)) {
			return;
		}

		String where = "(select product_id as productId,device_id as deviceId,country_zh as country,province_zh as province from iot_register.device_info " +
				"where create_time between '" + beginTime + "' and '" + endTime + "' ) as device_time_filter";

		Dataset<Row> todayDevice = new ReadTable().loadTable(spark, where).coalesce(1);
		Dataset<Row> yesterdayStats = spark.read().parquet(deviceTotal).select("productId", "totalNum");
		Dataset<Row> totalIncrement = todayDevice.groupBy("productId").agg(functions.countDistinct("deviceId").as("newNum"));

		Seq<String> seq = CommonUtil.columnNames("productId");
		Seq<String> naFillZero = CommonUtil.columnNames("newNum,totalNum");
		Dataset<Row> result = yesterdayStats.join(totalIncrement, seq, "outer").na().fill(0, naFillZero)
				.select(col("productId"), col("newNum"), col("newNum").plus(col("totalNum")).as("totalNum"))
				.withColumn("pt", lit(nowPt)).coalesce(1);

		Dataset<Row> yesterdayAreaStatistics = spark.read().parquet(deviceAreaTotal).select("productId", "country", "province", "totalNum").toDF();

		Dataset<Row> areaIncrement = todayDevice.groupBy("productId", "country", "province").agg(functions.countDistinct("deviceId").as("newNum"));

		seq = CommonUtil.columnNames("productId,country,province");
		Dataset<Row>  areaResult = yesterdayAreaStatistics.join(areaIncrement, seq, "outer").na().fill(0, naFillZero)
				.select(col("productId"), col("country"), col("province"), col("newNum"),
						col("newNum").plus(col("totalNum")).as("totalNum")).withColumn("pt", lit(nowPt)).coalesce(1);

		Dataset<DeviceArea> deltaArea;

		if (existsPath(originAreaPath)) {
			try {
				Dataset<Row> originBase = spark.read().parquet(originAreaPath);
				deltaArea = areaResult.except(originBase).coalesce(1).as(new DeviceArea().produceBeanEncoder());
			} catch (Exception e) {
				logger.error(e.getMessage());
				deltaArea = areaResult.as(new DeviceArea().produceBeanEncoder());
			}
		} else {
			deltaArea = areaResult.as(new DeviceArea().produceBeanEncoder());
		}

		try {
			insertToMysql(deltaArea);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		areaResult.write().mode("overwrite").format("parquet").save(originAreaPath);
		result.write().mode("overwrite").format("parquet").save(OnlineOfflinePath.ONLINE_DEVICE_NEW_TOTAL_PATH);

	}

	public void insertToMysql(Dataset<DeviceArea> dataSet) {
		dataSet.foreachPartition(data -> {
			String sql = "insert into stats_device_area(product_id,country,province,new_num,total_num,pt)" +
					"values (?,?,?,?,?,?) on duplicate key update new_num=?,total_num=?";
			new DeviceAreaOnlineSave().putDataBatch(data, sql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		DeviceAreaOnline deviceAreaOnline = new DeviceAreaOnline();
		deviceAreaOnline.runAll(pt, false);
	}
}
