package com.demo.online.streaming;

import com.demo.base.AbstractKafkaStreaming;
import com.demo.base.JavaSparkSessionSingleton;
import com.demo.bean.input.info.CheckInfo;
import com.demo.bean.input.info.DownInfo;
import com.demo.config.StationDownConfig;
import com.demo.bean.out.CheckDown;
import com.demo.bean.out.DownNum;
import com.demo.bean.out.DownRate;
import com.demo.common.sql.streaming.CheckDownNum;
import com.demo.common.sql.streaming.CheckDownRate;
import com.demo.util.DateUtil;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple4;

import static org.apache.spark.sql.functions.*;

/**
 * @author allen
 *         执行命令
 *         $SPARK_HOME/bin/spark-submit  --class "com.adups.online.streaming.StatsStationOnline"
 *         --total-executor-cores 5 --packages com.alibaba:fastjson:1.2.11,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1
 *         BigDataAnalysis-1.0.0.jar streamingCheckpoint
 */
public class StatsStationOnline extends AbstractKafkaStreaming {

	@Override
	public JavaStreamingContext createContext(JavaStreamingContext jssc, Map<String, Object> kafkaParams) {
		Collection<String> checkTopics = Arrays.asList("ota_check");
		Collection<String> downTopics = Arrays.asList("ota_download");
		kafkaParams.put("group.id", "online_station");
		JavaInputDStream<ConsumerRecord<String, String>> streamCheck = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(checkTopics, kafkaParams));

		JavaPairDStream<Tuple4, Tuple2> checkResult = streamCheck.mapToPair(
				s -> {
					try {
						CheckInfo checkInfo = JSON.parseObject(s.value().toString(), CheckInfo.class);
						String mid = checkInfo.getMid();
						Long productId = checkInfo.getProductId();
						String deviceId = checkInfo.getDeviceId();
						Integer deltaId = checkInfo.getDeltaId();
						String lac = checkInfo.getLac();
						String cid = checkInfo.getCid();
						if (lac.equals("") || cid.equals("")) {
							return new Tuple2<Tuple4, Tuple2>(null, null);
						}
						Tuple4<String, Long, String, Integer> tuple4 = new Tuple4<String, Long, String, Integer>(mid, productId, deviceId, deltaId);
						Tuple2<String, String> tuple2 = new Tuple2<String, String>(lac, cid);
						return new Tuple2<Tuple4, Tuple2>(tuple4, tuple2);

					} catch (Exception e) {
						return new Tuple2<Tuple4, Tuple2>(null, null);
					}
				}
		).filter(s -> s._2() != null && s._2()._1() != null && s._2._2 != null).window(Durations.minutes(10), Durations.minutes(1));

		JavaInputDStream<ConsumerRecord<String, String>> streamDown = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(downTopics, kafkaParams));

		JavaPairDStream<Tuple4, Float> downResult = streamDown.mapToPair(
				s -> {
					try {
						DownInfo downInfo = JSON.parseObject(s.value().toString(), DownInfo.class);
						Integer downloadStatus = downInfo.getDownloadStatus();
						if (downloadStatus != 1) {
							return new Tuple2<Tuple4, Float>(null, null);
						}
						Long downStart = Long.parseLong(downInfo.getDownStart());
						Long downEnd = Long.parseLong(downInfo.getDownEnd());
						String mid = downInfo.getMid();
						Long productId = downInfo.getProductId();
						String deviceId = downInfo.getDeviceId();
						Integer deltaId = downInfo.getDeltaId();
						Integer downSize = downInfo.getDownSize();
						Float downRate = (float) downSize / (downEnd - downStart) / 1024;
						Tuple4<String, Long, String, Integer> tuple4 = new Tuple4<String, Long, String, Integer>(mid, productId, deviceId, deltaId);
						return new Tuple2<Tuple4, Float>(tuple4, downRate);
					} catch (Exception e) {
						e.printStackTrace();
						return new Tuple2<Tuple4, Float>(null, null);
					}
				}
		).filter(s -> s._2 != null && s._2() > 0.01 && s._2 < 50)
				//过去五分钟数据,每一分钟算1次
				.window(Durations.minutes(5), Durations.minutes(1));

		JavaDStream<CheckDown> joinResult = checkResult.join(downResult).map(
				s -> {
					CheckDown checkDown = new CheckDown();
					Long productId = Long.parseLong(s._1._2().toString());
					String deviceId = s._1._3().toString();
					String lac = s._2()._1()._1().toString();
					String cid = s._2()._1()._2().toString();
					Float downRate = s._2()._2();
					checkDown.setProductId(productId);
					checkDown.setDeviceId(deviceId);
					checkDown.setLac(lac);
					checkDown.setCid(cid);
					checkDown.setDownRate(downRate);
					return checkDown;
				}
		);

		joinResult.foreachRDD((rdd, time) -> {
			SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
			Dataset<Row> checkOrDown = spark.createDataFrame(rdd, CheckDown.class);
			String rateTime = DateUtil.getMinuteTimeYmd();
			Dataset<Row> downAvg = checkOrDown.groupBy("productId", "lac", "cid")
					.agg(round(functions.sum("downRate"), 4).as("downRateSum"),
							functions.count("deviceId").as("countNum"),
							round(functions.sum("downRate").as("downAllRate")
									.divide(functions.count("deviceId").as("countNum")), 4).as("downRate"));
			Dataset<Row> result = downAvg.withColumn("createTime", functions.date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
					.withColumn("rateTime", lit(rateTime));

			DownRate downRate = new DownRate();
			Dataset<DownRate> downAvgResult = result.na().drop().as(downRate.produceBeanEncoder()).coalesce(1);

			if (downAvgResult.count() != 0) {
				try {
					insertMySQL(downAvgResult);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			Dataset<Row> downRates = checkOrDown.groupBy("lac", "cid").agg(round(functions.sum("downRate").as("downAllRate")
					.divide(functions.count("deviceId").as("countNum")), 4).as("downRate"));
			Dataset<Row> downNum = downRates.withColumn("nums",
					/*下载速率 < 1 Kb/s   -20 */
					when(col("downRate").lt(StationDownConfig.getKbsOne()), StationDownConfig.getJudgeOne())
							/*1 Kb/s =< 下载速率 < 2 Kb/s   -10 */
							.when(col("downRate").geq(StationDownConfig.getKbsOne()).and(col("downRate").lt(StationDownConfig.getKbsTwo())), StationDownConfig.getJudgeTwo())
							/*2 Kb/s =< 下载速率 < 5 Kb/s   0 */
							.when(col("downRate").geq(StationDownConfig.getKbsTwo()).and(col("downRate").lt(StationDownConfig.getKbsThree())), StationDownConfig.getJudgeThree())
							/*5 Kb/s =< 下载速率 < 10 Kb/s   +10 */
							.when(col("downRate").geq(StationDownConfig.getKbsThree()).and(col("downRate").lt(StationDownConfig.getKbsFour())), StationDownConfig.getJudgeFour())
							/*10 Kb/s =< 下载速率 < 20 Kb/s   +20 */
							.when(col("downRate").geq(StationDownConfig.getKbsFour()).and(col("downRate").lt(StationDownConfig.getKbsFive())), StationDownConfig.getJudgeFive())
							/*20 Kb/s =< 下载速率 < 50 Kb/s   +30 */
							.when(col("downRate").geq(StationDownConfig.getKbsFive()).and(col("downRate").lt(StationDownConfig.getKbsSix())), StationDownConfig.getJudgeSix())
							/*50 Kb/s =< 下载速率   +50 */
							.when(col("downRate").geq(StationDownConfig.getKbsSix()), StationDownConfig.getJudgeSeven()));

			Dataset<DownNum> downNumResult = downNum.na().drop().as(new DownNum().produceBeanEncoder()).coalesce(1);
			if (downNumResult.count() != 0) {
				try {
					insertOrUpdateMySQL(downNumResult);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		return jssc;
	}

	private static void insertMySQL(Dataset<DownRate> dataSet) {
		dataSet.foreachPartition(data -> {
			new CheckDownRate().putDataBatch(data);
		});
	}

	private static void insertOrUpdateMySQL(Dataset<DownNum> dataSet){
		dataSet.foreachPartition(data -> {
			new CheckDownNum().putDataBatch(data);
		});
	}

	public static void main(String[] args) throws InterruptedException {
		String checkpointDirectory = args[0];
		StatsStationOnline statsStationOnline = new StatsStationOnline();
		statsStationOnline.runAll(checkpointDirectory, Durations.seconds(30));
	}
}
