package com.adups.online.flume;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.ProductDeltaMerge;
import com.adups.config.FlumePath;
import com.adups.config.OnlineOfflinePath;
import com.adups.common.sql.flume.ProductDeltaMergeOnlineSave;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 10/08/2017.
 */
public class ProductDeltaMergeOnline extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String preNow=DateUtil.pathNowPtWithPre();
		String nowPt=DateUtil.nowPtDay();
		String checkPath= FlumePath.CHECK_PATH+preNow;
		String downPath=FlumePath.DOWN_PATH+preNow;
		String upgradePath=FlumePath.UPGRADE_PATH+preNow;
		String statsOrigin= OnlineOfflinePath.ONLINE_PRODUCT_DELTA_MERGE;

		Dataset<Row> checkData=null;
		Dataset<Row> downData=null;
		Dataset<Row> upgradeData=null;
		Dataset<Row> result=null;
		Dataset<ProductDeltaMerge> deltaData;

		try {
			if(fileSystem.exists(new Path(checkPath)) ) {
				checkData=spark.read().json(checkPath).groupBy("productId","deltaId","originVersion","nowVersion")
				.agg(functions.countDistinct("deviceId").as("countCheck"));
			}
			if(fileSystem.exists(new Path(downPath)) ) {

				WindowSpec w = Window.partitionBy("deviceId", "productId", "deltaId", "downloadStatus").orderBy(col("createTime").asc_nulls_last());
				downData = spark.read().json(downPath).withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank").distinct()
						.withColumn("countDownload", when(col("downloadStatus").equalTo(1), 1).otherwise(0))
						.withColumn("countDownFail", when(col("downloadStatus").notEqual(1), 1).otherwise(0))
						.groupBy("productId", "deltaId", "originVersion", "nowVersion").agg(sum("countDownload").as("countDownload"),
								sum("countDownFail").as("countDownFail"));
			}
			if(fileSystem.exists(new Path(upgradePath)) ) {
				WindowSpec w= Window.partitionBy("deviceId","productId","deltaId","updateStatus").orderBy(col("createTime").asc_nulls_last());
				upgradeData=spark.read().json(upgradePath).withColumn("rank",row_number().over(w)).where(col("rank").equalTo(1)).drop("rank").distinct()
						.withColumn("countUpgrade",when(col("updateStatus").equalTo(1),1).otherwise(0))
						.withColumn("countUpgradeFail",when(col("updateStatus").notEqual(1),1).otherwise(0))
						.groupBy("productId","deltaId","originVersion","nowVersion").agg(sum("countUpgrade").as("countUpgrade"),
								sum("countUpgradeFail").as("countUpgradeFail"));
			}

		} catch (IOException e) {
			logger.error(e.getMessage());
		}

		Seq<String> seq= CommonUtil.columnNames("productId,deltaId,originVersion,nowVersion");

		ArrayList<Dataset<Row>> allList=new ArrayList();

		//为什么三个 if 判断,因为有时候当天的目录为空.
		if(checkData!=null){
			allList.add(checkData);
		}
		if(downData!=null){
			allList.add(downData);
		}
		if(upgradeData!=null){
			allList.add(upgradeData);
		}

		for (Dataset<Row> tmp:allList){
			if(result==null){
				result=tmp;
			}else{
				result=result.join(tmp,seq,"outer");
			}
		}
		allList.clear();

		if(result==null){
			logger.warn("all the date in check ,down,upgrade is null ");
			return ;
		}
		if(checkData==null){
			result=result.withColumn("countCheck",lit(0));
		}
		if(downData==null){
			result=result.withColumn("countDownload",lit(0)).withColumn("countDownFail",lit(0));
		}
		if(upgradeData==null){
			result=result.withColumn("countUpgrade",lit(0)).withColumn("countUpgradeFail",lit(0));
		}

		String[] fillZeroColumns="countCheck,countDownload,countUpgrade,countDownFail,countUpgradeFail".split(",");
		result=result.select(col("productId").cast("string"),col("deltaId").cast("string"),
				col("originVersion"), col("nowVersion").as("targetVersion"), col("countCheck"),
				col("countDownload"), col("countDownFail"),col("countUpgrade"),col("countUpgradeFail"))
				.na().fill(0,fillZeroColumns).withColumn("pt",lit(nowPt)).coalesce(1);

		ProductDeltaMerge productDeltaMerge =new ProductDeltaMerge();
		if(existsPath(statsOrigin)){
			try {
				Dataset<Row> originBase = spark.read().parquet(statsOrigin);
				deltaData = result.except(originBase).na().drop().coalesce(1).as(productDeltaMerge.produceBeanEncoder());
			}catch (Exception e){
				logger.error(e.getMessage());
				deltaData=result.na().drop().as(productDeltaMerge.produceBeanEncoder()) ;
			}
		}else{
			deltaData=result.na().drop().as(productDeltaMerge.produceBeanEncoder()) ;
		}

		try {
			insertMysql(deltaData);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		result.write().mode("overwrite").format("parquet").save(statsOrigin);
	}

	public void insertMysql(Dataset<ProductDeltaMerge> dataSet) {
		dataSet.foreachPartition(data -> {
			new ProductDeltaMergeOnlineSave().putDataBatch(data);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		ProductDeltaMergeOnline productDeltaMergeOnline =new ProductDeltaMergeOnline();
		productDeltaMergeOnline.runAll(pt,false);
	}
}
