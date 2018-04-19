package com.adups.offline.save.app;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.AppCountUpgrade;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.app.AppCountUpgradeSave;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import java.io.IOException;
import java.sql.SQLException;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountUpgradeData extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {
		String checkSql="select product_id,package_name,count(distinct device_id) as check_num " +
				"from app_check_new  where  pt='" + pt + "' and new_type='1' group by product_id,package_name";
		logger.warn(checkSql);
		Dataset<Row> checkNum=spark.sql(checkSql).repartition(1);

		String downUpgradeSql="select product_id,package_name,status,count(distinct device_id) as num from " +
				"app_check_report where  pt='" + pt + "' and status in ('1000','2000') group by product_id,package_name,status";
		logger.warn(downUpgradeSql);
		Dataset<Row> downUpNum=spark.sql(downUpgradeSql).repartition(1);

		Dataset<Row> downNum=downUpNum.filter(col("status").equalTo("1000")).drop("status").withColumnRenamed("num","down_num");
		Dataset<Row> upNum=downUpNum.filter(col("status").equalTo("2000")).drop("status").withColumnRenamed("num","up_num");

		Seq<String> seq= CommonUtil.columnNames("product_id,package_name");
		String[] fillZeroColumns="check_num,down_num,up_num".split(",");
		Dataset<Row> appCountUpgradeData=checkNum.join(downNum,seq,"outer").join(upNum,seq,"outer").na()
				.fill(0,fillZeroColumns).withColumn("pt",lit(pt));

		AppCountUpgrade appCountUpgrade=new AppCountUpgrade();
		@SuppressWarnings("unchecked")
		Dataset<AppCountUpgrade> appCountUpgradeSet = (Dataset<AppCountUpgrade>) appCountUpgradeData.withColumnRenamed("product_id","productId")
				.withColumnRenamed("package_name","packageName").withColumnRenamed("check_num","checkNum")
				.withColumnRenamed("down_num","downNum").withColumnRenamed("up_num","upNum")
				.coalesce(1).as(appCountUpgrade.produceBeanEncoder());
		try {
			insertToMysql(appCountUpgradeSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertToMysql(Dataset<AppCountUpgrade> dataSet, String pt) throws SQLException {
		String sql = "delete from app_count_upgrade_data where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into app_count_upgrade_data set product_id=?,package_name=?,check_num=?,down_num=?," +
					"up_num=?,pt=?";
			new AppCountUpgradeSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		AppCountUpgradeData appCountVersionData =new AppCountUpgradeData();
		appCountVersionData.runAll(pt);
	}
}
