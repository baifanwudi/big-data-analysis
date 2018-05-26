package com.demo.offline.save.app;

import com.demo.base.AbstractSparkSql;
import com.demo.bean.out.AppCountArea;
import com.demo.common.BeforeBatchPut;
import com.demo.common.sql.app.AppCountAreaSave;
import com.demo.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.spark.sql.functions.lit;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountAreaData extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) throws IOException {

		String sql="select product_id,package_name,country_en,country_zh ,province_en," +
				"province_zh,count(distinct device_id) as total_num from " +
				"app_register_info  where pt='" + pt + "' group by product_id,package_name,country_en,country_zh,province_en,province_zh";
		logger.warn(sql);

		AppCountArea appCountArea=new AppCountArea();
		@SuppressWarnings("unchecked")
		Dataset<AppCountArea> appCountAreaSet = (Dataset<AppCountArea>) spark.sql(sql).withColumnRenamed("product_id","productId")
				.withColumnRenamed("package_name","packageName").withColumnRenamed("country_en","countryEn")
				.withColumnRenamed("country_zh","countryZh")
				.withColumnRenamed("province_en","provinceEn").withColumnRenamed("province_zh","provinceZh")
				.withColumnRenamed("total_num","totalNum").withColumn("pt",lit(pt))
				.coalesce(1).as(appCountArea.produceBeanEncoder());
		try {
			insertToMysql(appCountAreaSet, pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertToMysql(Dataset<AppCountArea> dataSet, String pt) throws SQLException {
		String sql = "delete from app_count_area_data where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into app_count_area_data set product_id=?,package_name=?,country_en=?,country_zh=?,province_en=?," +
					"province_zh=?,total_num=?,pt=?";
			new AppCountAreaSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		AppCountAreaData appCountAreaData =new AppCountAreaData();
		appCountAreaData.runAll(pt);
	}
}
