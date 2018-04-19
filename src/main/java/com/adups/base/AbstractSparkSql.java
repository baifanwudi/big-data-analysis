package com.adups.base;


import com.adups.config.HiveConfig;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * @author allen
 * Created by allen on 04/08/2017.
 */
public abstract class AbstractSparkSql extends AbstractFileSystem {

	private  Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * spark运算
	 * @param pt 时间格式 pt=2017-10-11
	 * @param path hdfs路径
	 * @param spark
	 * @throws IOException
	 */
	public abstract void  executeProgram(String pt,String path,SparkSession spark) throws IOException;

	public boolean existsPath(String... pathList) throws IOException {
		for (String path : pathList) {
			if (!fileSystem.exists(new Path(path))) {
				logger.error(" the path:" + path + " is not existed");
				return false;
			}else{
				logger.warn("executing the path is : " + path);
			}
		}
		return true;
	}

	public void runAll(String pt,String path,Boolean isHiveSupport) throws IOException {
		if(path!=null && !existsPath(path)) {
			logger.error("the src path is not existed:" + path);
			return;
		}
		executeSpark(pt,path,isHiveSupport);
	}

	/**
	 * 	没有路径判断,默认激活 hive
	 */
	public void runAll(String pt) throws IOException {
		runAll(pt,null,true);
	}

	public void runAll(String pt,String path) throws IOException {
		runAll(pt,path,true);
	}

	public void runAll(String pt,Boolean isHiveSupport) throws IOException {
		runAll(pt,null,isHiveSupport);
	}

	private void executeSpark(String pt,String path,Boolean isHiveSupport) throws IOException {

		SparkSession spark ;
		String appName=this.getClass().getSimpleName();
		if(isHiveSupport) {
			spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate();
			logger.info("spark enable hive, begin to execute the program");
			useDataBase(spark);
		}else{
			spark = SparkSession.builder().appName(appName).getOrCreate();
			logger.info("spark begin to execute the program");
		}
		executeProgram(pt,path,spark);
		logger.info("spark has finished the program ");
		//取消spark.stop,减少spark-session多次启动
//		spark.stop();
	}

	private void useDataBase(SparkSession spark){
		logger.info("before the sql : "+HiveConfig.SQL_DATABASE );
		spark.sql(HiveConfig.SQL_DATABASE);
	}

	public void beforePartition(SparkSession spark){
		spark.sql(HiveConfig.HIVE_PARTITION);
	}
}
