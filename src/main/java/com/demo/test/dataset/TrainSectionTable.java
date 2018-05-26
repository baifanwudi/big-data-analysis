package com.demo.test.dataset;


import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author allen
 */
public class TrainSectionTable {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("app-train").master("local[*]").getOrCreate();


        Dataset<Row> trainData =spark.read().json("src/main/data/train_stopover.txt").orderBy("duration_date","station_sequence");
        trainData.printSchema();
        /**
         *  |-- arrive_time: string (nullable = true)
         *  |-- duration_date: string (nullable = true)
         *  |-- leave_time: string (nullable = true)
         *  |-- station_code: string (nullable = true)
         *  |-- station_name: string (nullable = true)
         *  |-- station_sequence: long (nullable = true)
         *  |-- stopover_time: string (nullable = true)
         *  |-- train_no: string (nullable = true)
         *  |-- transport_code: string (nullable = true)
         */
        trainData.show();
        /**
         *+-----------+-------------+----------+------------+------------+----------------+-------------+------------+--------------+
         * |arrive_time|duration_date|leave_time|station_code|station_name|station_sequence|stopover_time|    train_no|transport_code|
         * +-----------+-------------+----------+------------+------------+----------------+-------------+------------+--------------+
         * |           |     20180515|     20:00|         BXP|         北京西|               1|             |2400000Z210A|           Z21|
         * |      22:33|     20180515|     22:37|         VVP|        石家庄北|               2|            4|2400000Z210A|           Z21|
         * |      00:19|     20180515|     00:25|         TYV|          太原|               3|            6|2400000Z210A|           Z21|
         * |      07:05|     20180515|     07:16|         ZWJ|          中卫|               4|           11|2400000Z210A|           Z21|
         * |      12:17|     20180515|     12:33|         LZJ|          兰州|               5|           16|2400000Z210A|           Z21|
         * |      15:01|     20180515|     15:21|         XNO|          西宁|               6|           20|2400000Z210A|           Z21|
         * |      19:23|     20180515|     19:25|         DHO|         德令哈|               7|            2|2400000Z210A|           Z21|
         * |      22:10|     20180515|     22:35|         GRO|         格尔木|               8|           25|2400000Z210A|           Z21|
         * |      08:18|     20180515|     08:24|         NQO|          那曲|               9|            6|2400000Z210A|           Z21|
         * |      12:20|     20180515|     12:20|         LSO|          拉萨|              10|             |2400000Z210A|           Z21|
         * |           |     20180516|     20:00|         BXP|         北京西|               1|             |2400000Z210A|           Z21|
         * |      22:33|     20180516|     22:37|         VVP|        石家庄北|               2|            4|2400000Z210A|           Z21|
         * |      00:19|     20180516|     00:25|         TYV|          太原|               3|            6|2400000Z210A|           Z21|
         * |      07:05|     20180516|     07:16|         ZWJ|          中卫|               4|           11|2400000Z210A|           Z21|
         * |      12:17|     20180516|     12:33|         LZJ|          兰州|               5|           16|2400000Z210A|           Z21|
         * |      15:01|     20180516|     15:21|         XNO|          西宁|               6|           20|2400000Z210A|           Z21|
         * |      19:23|     20180516|     19:25|         DHO|         德令哈|               7|            2|2400000Z210A|           Z21|
         * |      22:10|     20180516|     22:35|         GRO|         格尔木|               8|           25|2400000Z210A|           Z21|
         * |      08:18|     20180516|     08:24|         NQO|          那曲|               9|            6|2400000Z210A|           Z21|
         * |      12:20|     20180516|     12:20|         LSO|          拉萨|              10|             |2400000Z210A|           Z21|
         * +-----------+-------------+----------+------------+------------+----------------+-------------+------------+--------------+
         */


        Encoder<Tuple3<String, String, String>> tuple3Encoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING());
        Dataset<Row> result = trainData.groupByKey((MapFunction<Row, Tuple3<String, String, String>>) row -> {
            String transportCode = row.<String>getAs("transport_code");
            String durationDate = row.<String>getAs("duration_date");
            String trainNo = row.<String>getAs("train_no");
            return new Tuple3<String, String, String>(transportCode, durationDate, trainNo);
        }, tuple3Encoder)
                .flatMapGroups((FlatMapGroupsFunction<Tuple3<String, String, String>, Row, TrainSection>) (key, values) -> {
                    try {
                        Map<Integer, Row> originData = new HashMap<>();
                        int max = 0;
                        while (values.hasNext()) {
                            Row value = values.next();
                            int sequence = value.<Long>getAs("station_sequence").intValue();
                            if (sequence > max) {
                                max = sequence;
                            }
                            originData.put(sequence, value);
                        }
                        List<TrainSection> trainSections = new ArrayList<>();
                        TrainSection trainSection;
                        String transportCode = key._1();
                        String durationDate = key._2();
                        String trainNo = key._3();
                        for (int i = 1; i <= max - 1; i++) {
                            for (int j = i + 1; j <= max; j++) {
                                trainSection = new TrainSection();
                                trainSection.setTransportCode(transportCode);
                                trainSection.setTrainNode(trainNo);
                                trainSection.setDurationDate(durationDate);
                                trainSection.setBeginId(i);
                                trainSection.setBeginStationName(originData.get(i).<String>getAs("station_name"));
                                trainSection.setBeginStationCode(originData.get(i).<String>getAs("station_code"));
                                trainSection.setBeginTime(originData.get(i).<String>getAs("leave_time"));
                                trainSection.setEndId(j);
                                trainSection.setEndStationCode(originData.get(j).<String>getAs("station_code"));
                                trainSection.setEndStationName(originData.get(j).<String>getAs("station_name"));
                                trainSection.setEndTime(originData.get(j).<String>getAs("arrive_time"));
                                trainSections.add(trainSection);
                            }
                        }
                        return trainSections.iterator();
                    } catch (Exception e) {
                        return new ArrayList<TrainSection>().iterator();
                    }
                }, new TrainSection().produceBeanEncoder()).toDF();

        result.show(100);
        /**
         * +-------+----------------+----------------+---------+------------+-----+--------------+--------------+-------+------------+-------------+
         * |beginId|beginStationCode|beginStationName|beginTime|durationDate|endId|endStationCode|endStationName|endTime|   trainNode|transportCode|
         * +-------+----------------+----------------+---------+------------+-----+--------------+--------------+-------+------------+-------------+
         * |      1|             BXP|             北京西|    20:00|    20180518|    2|           VVP|          石家庄北|  22:33|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    3|           TYV|            太原|  00:19|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    3|           TYV|            太原|  00:19|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180518|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180518|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180518|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180518|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180518|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180518|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      7|             DHO|             德令哈|    19:25|    20180518|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      7|             DHO|             德令哈|    19:25|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      7|             DHO|             德令哈|    19:25|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      8|             GRO|             格尔木|    22:35|    20180518|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      8|             GRO|             格尔木|    22:35|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      9|             NQO|              那曲|    08:24|    20180518|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    2|           VVP|          石家庄北|  22:33|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    3|           TYV|            太原|  00:19|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    3|           TYV|            太原|  00:19|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      3|             TYV|              太原|    00:25|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180528|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180528|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180528|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      4|             ZWJ|              中卫|    07:16|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180528|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180528|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      5|             LZJ|              兰州|    12:33|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180528|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      6|             XNO|              西宁|    15:21|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      7|             DHO|             德令哈|    19:25|    20180528|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      7|             DHO|             德令哈|    19:25|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      7|             DHO|             德令哈|    19:25|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      8|             GRO|             格尔木|    22:35|    20180528|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      8|             GRO|             格尔木|    22:35|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      9|             NQO|              那曲|    08:24|    20180528|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    2|           VVP|          石家庄北|  22:33|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    3|           TYV|            太原|  00:19|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    4|           ZWJ|            中卫|  07:05|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    5|           LZJ|            兰州|  12:17|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    6|           XNO|            西宁|  15:01|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    7|           DHO|           德令哈|  19:23|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    8|           GRO|           格尔木|  22:10|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|    9|           NQO|            那曲|  08:18|2400000Z210A|          Z21|
         * |      1|             BXP|             北京西|    20:00|    20180530|   10|           LSO|            拉萨|  12:20|2400000Z210A|          Z21|
         * |      2|             VVP|            石家庄北|    22:37|    20180530|    3|           TYV|            太原|  00:19|2400000Z210A|          Z21|
         * +-------+----------------+----------------+---------+------------+-----+--------------+--------------+-------+------------+-------------+
         * only showing top 100 rows
         */

        spark.stop();
    }
}

