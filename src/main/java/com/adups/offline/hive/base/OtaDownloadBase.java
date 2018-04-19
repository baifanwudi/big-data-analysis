package com.adups.offline.hive.base;

import com.adups.config.OnlineOfflinePath;
import com.adups.util.CommonUtil;
import com.adups.util.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 * Created by allen on 02/08/2017.
 */
public class OtaDownloadBase  extends AbstractTimeFilter {

	private Logger logger = LoggerFactory.getLogger(OtaDownloadBase.class);

	@Override
	public void executeProgram(String pt, String path, SparkSession spark) {
		parseDownloadLog(spark, pt);
		completeMissingData(spark, pt);
	}

	private void parseDownloadLog(SparkSession spark, String pt){
		String ptWeek=DateUtil.ptAfterOrBeforeDay(pt,-7);

		Dataset<Row> sqlResult=spark.sql("select * from ota_interface_download_info_log where pt>='"+ptWeek+"' and pt<='"+pt+"' and create_time is not null and" +
				" product_id is not null and delta_id is not null and origin_version is not null and now_version is not null");
		//只取七天内create_time 最早那条记录,且只取 create_time like pt 记录
		WindowSpec w= Window.partitionBy("device_id","product_id","delta_id","download_status").orderBy(col("create_time").asc_nulls_last());
		Dataset<Row> down=sqlResult.withColumn("rank",row_number().over(w)).where(col("rank").equalTo(1)).drop("rank");
		Dataset<Row> downParse=filterTimeIsNotPt(down,true,pt).distinct().repartition(1);
		if(downParse.count()==0){
			logger.error("the count of ota_interface_download_info_log in "+pt+" is 0 ");
			return ;
		}

		downParse.createOrReplaceTempView("OtaDownloadBase");
		beforePartition(spark);
		String sql=" insert overwrite table stats_interface_download_info_base partition(pt ='"+pt+"') select mid,device_id,product_id," +
				"origin_version,now_version,down_start,down_end,delta_id,download_status,create_time,ip,province,apn_type,city,ext_str," +
				"down_size,down_ip,data_type from OtaDownloadBase ";
		logger.warn(sql);
		spark.sql(sql);
	}

	private void completeMissingData(SparkSession spark, String pt ){
		logger.warn(" begin to complete the missing DownBaseLog");
		Dataset<Row> filterCheckBase=spark.read().parquet(OnlineOfflinePath.OFFLINE_MISSING_DATA_TMP_PATH);
		if(filterCheckBase.count()==0){
			logger.error("the count of CompleteMissingData in "+pt+" is 0 ");
			return ;
		}
		String ptWeek=DateUtil.ptAfterOrBeforeDay(pt,-7);
		Dataset<Row> filterDown=spark.sql("select * from stats_interface_download_info_base where pt>='"+ptWeek+"' and pt<='"+pt+"' and download_status='1' ");
			Seq<String> seq= CommonUtil.columnNames("mid,device_id,product_id,delta_id,origin_version,now_version");
		Dataset<Row> result=filterCheckBase.join(filterDown,seq,"left").filter(filterDown.col("pt").isNull()).
				select(filterCheckBase.col("*")).repartition(1);
		result.createOrReplaceTempView("DownComplement");
		beforePartition(spark);

		String sql=" insert into table stats_interface_download_info_base partition(pt ='"+pt+"') select mid,device_id,product_id," +
				"origin_version,now_version,null,null,delta_id,1,create_time,ip,province,0,city,'lost'," +
				"null,null ,data_type from DownComplement ";
		logger.warn(sql);
		spark.sql(sql);
	}

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		OtaDownloadBase otaDownloadBase=new OtaDownloadBase();
		otaDownloadBase.runAll(pt);
	}
}
