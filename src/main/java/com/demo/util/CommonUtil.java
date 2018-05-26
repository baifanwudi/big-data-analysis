package com.demo.util;

import org.apache.spark.sql.Dataset;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import java.util.Arrays;
import java.util.List;

/**
 * @author allen
 * Created by allen on 25/07/2017.
 */
public final class CommonUtil {

	public static Option<Seq<String>> columnNames(List<String> listName){
		Seq<String> b=(scala.collection.Seq<String>) listName;
		return Option.apply(b);
	}

	public static Seq<String> columnNames(String columnsName){
		List<String> list= Arrays.asList(columnsName.split(","));
		return JavaConversions.asScalaBuffer(list);
	}

	public boolean hasColumn(Dataset dataset, String colName){
		return Arrays.asList(dataset.columns()).contains(colName);
	}
}
