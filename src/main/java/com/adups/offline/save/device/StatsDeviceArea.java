package com.adups.offline.save.device;

import com.adups.base.AbstractSparkSql;
import com.adups.bean.out.DeviceArea;
import com.adups.common.BeforeBatchPut;
import com.adups.common.sql.device.DeviceAreaSave;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 09/08/2017.
 */
public class StatsDeviceArea extends AbstractSparkSql {

	private Logger logger = LoggerFactory.getLogger(this.getClass());
	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {

		String sql="select product_id as productId,country,province,new_num as newNum,total_num as totalNum" +
				",pt from stats_device_area where pt='"+pt+"'";
		logger.warn(sql);
		DeviceArea deviceArea=new DeviceArea();
		@SuppressWarnings("unchecked")
		Dataset<DeviceArea> deviceAreaDataSet=(Dataset<DeviceArea> )spark.sql(sql).as(deviceArea.produceBeanEncoder());
		try {
			insertToMysql(deviceAreaDataSet,pt);
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	public void insertToMysql(Dataset<DeviceArea> dataSet, String pt) throws SQLException {
		String sql = "delete from  stats_device_area where pt='" + pt + "'";
		new BeforeBatchPut().executeSqlBeforeBatch(sql);
		dataSet.foreachPartition(data -> {
			String insertMysql = "insert into stats_device_area set product_id=?,country=?," +
					"province=?,new_num=?,total_num=?,pt=?";
			new DeviceAreaSave().putDataBatch(data,insertMysql);
		});
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		StatsDeviceArea statsDeviceArea=new StatsDeviceArea();
		statsDeviceArea.runAll(pt);
	}
}
