package com.adups.offline.hive.base;

import com.adups.base.AbstractSparkSql;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

/**
 * @author allen
 * Created by allen on 13/10/2017.
 */
public abstract class AbstractTimeFilter extends AbstractSparkSql {

	public Dataset<Row> filterTimeIsNotPt (Dataset<Row> sqlResult, Boolean isFilter, String pt){

		if(isFilter){
			return sqlResult.filter(col("create_time").like(pt + "%"));
		}else{
			return sqlResult;
		}
	}
}
