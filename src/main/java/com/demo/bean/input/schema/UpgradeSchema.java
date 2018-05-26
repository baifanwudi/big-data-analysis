package com.demo.bean.input.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author allen
 * Created by allen on 07/09/2017.
 */
public class UpgradeSchema {

	public StructType produceSchema(){
		List<StructField> inputFields=new ArrayList<>();
		String splitSeq=",";
		String stringType="apnType,extStr,mid,deviceId,originVersion,nowVersion,ip,province,city,dataType";
		String timeType="createTime";
		String integerType="updateStatus,downSize";
		String longType="productId,deltaId";
		for(String stringTmp:stringType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,false));
		for(String integerTmp:integerType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(integerTmp,DataTypes.IntegerType,true));
		}
		for(String longTmp:longType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
		}
		return DataTypes.createStructType(inputFields);
	}
}
