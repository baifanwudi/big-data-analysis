package com.demo.ml;


import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class PipeLineLoad {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPipelineExample")
                .master("local[*]")
                .getOrCreate();


        PipelineModel pipelineModel = PipelineModel.load("model/final/pipemodel");
        Dataset<Row> test=spark.read().json("/Users/AllenBai/Downloads/train_data.json").limit(1);
        test.show();
        Dataset<Row> predictions = pipelineModel.transform(test);
//                .select("features","probability","prediction");

        predictions.show(false);

        spark.stop();
    }
}
