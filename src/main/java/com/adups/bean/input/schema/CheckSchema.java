package com.adups.bean.input.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author allen
 * Created by allen on 07/09/2017.
 */
public class CheckSchema {

	public StructType produceSchema(){
		List<StructField> inputFields=new ArrayList<>();
		String splitSeq=",";
		String stringType="networkType,lac,cid,mcc,mnc,rxlev,mid,deviceId,originVersion,nowVersion,ip,province,city,dataType";
		String timeType="createTime";
		String longType="productId,deltaId";
		for(String stringTmp:stringType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
		}
		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,true));
		for(String longTmp:longType.split(splitSeq)){
			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
		}
		return DataTypes.createStructType(inputFields);
	}
}
