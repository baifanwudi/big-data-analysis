package com.demo.offline.save.app;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.out.AppCountUser;
import com.demo.common.BeforeBatchPut;
import com.demo.common.sql.app.AppCountUserSave;
import com.demo.util.CommonUtil;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.spark.sql.functions.lit;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountUserData extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String newSql="select product_id,package_name,count(distinct device_id) as new_num from app_register_info  where  " +
				" pt='" + pt + "' group by product_id,package_name";
		logger.warn(newSql);
		Dataset<Row> userNew=spark.sql(newSql);

		String activeSql="select product_id,package_name,count(distinct device_id) as active_num from app_check_log where " +
				"  pt='" + pt + "' group by product_id,package_name";
		logger.warn(activeSql);
		Dataset<Row> userActive=spark.sql(activeSql);

		Seq<String> seq= CommonUtil.columnNames("product_id,package_name");
		Seq<String> naFillZero= CommonUtil.columnNames("new_num,active_num");
		Dataset<Row> appCountUserData=userNew.join(userActive,seq,"outer").na().fill(0,naFillZero).withColumn("pt",lit(pt));

		AppCountUser appCountUser=new AppCountUser();
		@SuppressWarnings("unchecked")
		Dataset<AppCountUser> appCountUserSet = (Dataset<AppCountUser>) appCountUserData.withColumnRenamed("product_id","productId")
				.withColumnRenamed("package_name","packageName").withColumnRenamed("new_num","newNum")
				.withColumnRenamed("active_num","activeNum").coalesce(1).as(appCountUser.produceBeanEncoder());
		try {
			insertToMysql(appCountUserSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertToMysql(Dataset<AppCountUser> dataSet, String pt) throws SQLException {
		String sql = "delete from app_count_user_data where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into app_count_user_data set product_id=?,package_name=?,new_num=?,active_num=?,pt=?";
			new AppCountUserSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		AppCountUserData appCountAreaData =new AppCountUserData();
		appCountAreaData.runAll(pt);
	}
}
