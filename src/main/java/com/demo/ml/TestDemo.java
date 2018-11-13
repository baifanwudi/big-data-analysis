package com.demo.ml;

import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class TestDemo {

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaDecisionTreeClassificationExample")
                .master("local[*]")
                .getOrCreate();


        StructType schema = createStructType(new StructField[]{
                createStructField("userid ", StringType, false),
                createStructField("tickettotalprice", DoubleType, false),
                createStructField("comboprice", IntegerType, false),
                createStructField("buytype", IntegerType, false),
                createStructField("runtime", IntegerType, false),
                createStructField("trainno", StringType, false),
                createStructField("traintype", StringType, false),
                createStructField("startstation", StringType, false),
                createStructField("endstation", StringType, false),
                createStructField("startdate", StringType, false),
                createStructField("enddate", StringType, false),
                createStructField("city", StringType, false),
                createStructField("isgoumai", IntegerType, false),
                createStructField("iscarcombo", IntegerType, false),
                createStructField("ishotelcombo", IntegerType, false),
                createStructField("isscenecombo", IntegerType, false),
                createStructField("modeldataaheadbooktime", IntegerType, false),
                createStructField("modeldataarrivehour", IntegerType, false),
                createStructField("modeldataarrivehomeflag", DoubleType, false),
                createStructField("modeldataarrivetime", DoubleType, false),
                createStructField("modeldataboughtsceneryticketflag", IntegerType, false),
                createStructField("modeldatabuytype", IntegerType, false),
                createStructField("modeldatacityscenerycount", IntegerType, false),
                createStructField("modeldatatravelflag", IntegerType, false),
                createStructField("modeldatapassengerage", IntegerType, false),
                createStructField("modeldataseatclass", IntegerType, false),
                createStructField("modeldatatraintype", IntegerType, false),
                createStructField("modeldatadeparturehour", IntegerType, false),
                createStructField("modeldatadepartureminute", IntegerType, false),
                createStructField("modeldataarrivecityid", IntegerType, false),
                createStructField("modeldataorderrfm", IntegerType, false),
                createStructField("modeldatainsurancerfm", IntegerType, false),
                createStructField("modeldatatakencarflag", IntegerType, false),
                createStructField("modeldatabookedhotelflag", IntegerType, false),
                createStructField("modeldataweixinfrequency", IntegerType, false),
                createStructField("modeldataregisteredyear", IntegerType, false),
                createStructField("modeldatabookdatehour", DoubleType, false),
                createStructField("modeldataaheadbookminute", IntegerType, false),
                createStructField("modeldatacity5ascenerycount", IntegerType, false),
                createStructField("modeldatalastorderdepartdatetype", IntegerType, false),
                createStructField("modeldatalastorderarrivedatetype", IntegerType, false),
                createStructField("modeldatalastorderarrivedatetime", DoubleType, false),
                createStructField("modeldatalastorderpassengercount", IntegerType, false),
                createStructField("modeldatalastorderarrivalcityissame", IntegerType, false),
                createStructField("modeldatalastorderinterval", IntegerType, false),
                createStructField("modeldatacouponsoperation7day", IntegerType, false),
                createStructField("modeldatacouponsoperation30day", IntegerType, false),
                createStructField("modeldatacouponsoperation90day", IntegerType, false),
                createStructField("modeldatacouponsoperation180day", IntegerType, false),
                createStructField("modeldatacouponsoperation360day", IntegerType, false),
                createStructField("modeldatabuycouponstype", IntegerType, false),
                createStructField("modeldatabuycouponsamount", IntegerType, false),
                createStructField("modeldatahotelrfm", IntegerType, false),
                createStructField("modeldatacarrfm", IntegerType, false),
                createStructField("modeldatasceneryrfm", IntegerType, false),
                createStructField("modeldatabookdatedayofweek", IntegerType, false),
                createStructField("modeldatadepartdatedayofweek", IntegerType, false),
                createStructField("modeldataarrivaldatedayofweek", IntegerType, false),
                createStructField("modeldataisfamiliarplace", IntegerType, false),
                createStructField("trainsearchcount2h", IntegerType, false),
                createStructField("departdatechangecount2h", IntegerType, false),
                createStructField("cancelcouponscount10m", IntegerType, false),
                createStructField("cancelcouponscount2h", IntegerType, false),
                createStructField("carsearchcount2h", IntegerType, false)
        });
        Dataset<Row> result = spark.read()
                .schema(schema)
                .option("header", "true").csv("/Users/AllenBai/Downloads/base/train.csv");

        result.show();
        result.printSchema();

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("isgoumai")
                .setOutputCol("label")
                .fit(result);
        StringIndexerModel trainType = new StringIndexer()
                .setInputCol("traintype")
                .setOutputCol("trainTypeIndex")
                .fit(result);
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"tickettotalprice","trainTypeIndex","comboprice", "buytype", "runtime", "iscarcombo", "ishotelcombo", "isscenecombo", "modeldataaheadbooktime", "modeldataarrivehour", "modeldataarrivehomeflag", "modeldataarrivetime",
                        "modeldataboughtsceneryticketflag", "modeldatabuytype", "modeldatacityscenerycount", "modeldatatravelflag", "modeldatapassengerage", "modeldataseatclass", "modeldatatraintype", "modeldatadeparturehour", "modeldatadepartureminute"
                        , "modeldataorderrfm", "modeldatainsurancerfm", "modeldatatakencarflag", "modeldatabookedhotelflag", "modeldataweixinfrequency", "modeldataregisteredyear", "modeldatabookdatehour", "modeldataaheadbookminute", "modeldatacity5ascenerycount",
                "modeldatalastorderdepartdatetype","modeldatalastorderarrivedatetype","modeldatalastorderarrivedatetime","modeldatalastorderpassengercount","modeldatalastorderarrivalcityissame","modeldatalastorderinterval",
                        "modeldatacouponsoperation7day","modeldatacouponsoperation30day","modeldatacouponsoperation90day","modeldatacouponsoperation180day","modeldatacouponsoperation360day","modeldatabuycouponstype",
                "modeldatabuycouponsamount"})
                .setOutputCol("features");

//        VectorIndexer featureIndexer = new VectorIndexer()
//                .setInputCol("features")
//                .setOutputCol("indexedFeatures")
//                .setMaxCategories(10);
//                .fit(result);
//        Dataset<Row> output = assembler.transform(result).select("features", "isgoumai");
//        output.show();


        Map map=new HashMap<>();
        map.put("objective","binary:logistic");
        map.put("eta",0.1);
        map.put("max_depth",9);
        map.put("min_child_weight",5);
        map.put("missing",-1);
        map.put("alpha",1);
        map.put("eval_metric","logloss");
        map.put("num_round","20");
        XGBoostClassifier xgBoostClassifier=
                new XGBoostClassifier()
                .setLabelCol("label")
                .setFeaturesCol("features");


        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        //featureIndexer
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer,trainType,assembler,xgBoostClassifier,labelConverter});
        PipelineModel pipelineModel=pipeline.fit(result);
        pipelineModel.write().overwrite().save("model/final/pipemodel");
        spark.stop();
    }
}
