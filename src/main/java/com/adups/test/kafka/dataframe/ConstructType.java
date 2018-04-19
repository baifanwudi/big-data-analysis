package com.adups.test.kafka.dataframe;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by allen on 13/07/2017.
 */
public class ConstructType {

	public StructType produceSchema(){
		StructField originVersion= DataTypes.createStructField("origin_version",DataTypes.StringType,true);
		StructField nowVersion= DataTypes.createStructField("now_version",DataTypes.StringType,true);
		StructField createTime= DataTypes.createStructField("create_time",DataTypes.StringType,true);

		List<StructField> fieldList=new ArrayList<>();
		fieldList.add(originVersion);
		fieldList.add(nowVersion);
		fieldList.add(createTime);

		return DataTypes.createStructType(fieldList);
	}
}
