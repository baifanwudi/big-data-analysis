package com.adups.config;

import org.apache.spark.SparkFiles;
import java.io.*;
import java.util.Properties;

/**
 * @author allen
 */
public class StationDownConfig implements Serializable {

	private static Properties properties = new Properties();

	private static int num;

	private static int kbsOne;

	private static int judgeOne;

	private static int kbsTwo;

	private static int judgeTwo;

	private static int kbsThree;

	private static int judgeThree;

	private static int kbsFour;

	private static int judgeFour;

	private static int kbsFive;

	private static int judgeFive;

	private static int kbsSix;

	private static int judgeSix;

	private static int judgeSeven;

	static {
		try {
			InputStream inputStream = new FileInputStream(new File(SparkFiles.get("station-download.properties")));
			properties.load(inputStream);
			num = Integer.parseInt(properties.getProperty("down.num"));
			kbsOne = Integer.parseInt(properties.getProperty("down.kbsOne"));
			judgeOne = Integer.parseInt(properties.getProperty("down.judgeOne"));
			kbsTwo = Integer.parseInt(properties.getProperty("down.kbsTwo"));
			judgeTwo = Integer.parseInt(properties.getProperty("down.judgeTwo"));
			kbsThree = Integer.parseInt(properties.getProperty("down.kbsThree"));
			judgeThree = Integer.parseInt(properties.getProperty("down.judgeThree"));
			kbsFour = Integer.parseInt(properties.getProperty("down.kbsFour"));
			judgeFour = Integer.parseInt(properties.getProperty("down.judgeFour"));
			kbsFive = Integer.parseInt(properties.getProperty("down.kbsFive"));
			judgeFive = Integer.parseInt(properties.getProperty("down.judgeFive"));
			kbsSix = Integer.parseInt(properties.getProperty("down.kbsSix"));
			judgeSix = Integer.parseInt(properties.getProperty("down.judgeSix"));
			judgeSeven = Integer.parseInt(properties.getProperty("down.judgeSeven"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static int getNum() {
		return num;
	}

	public static void setNum(int num) {
		StationDownConfig.num = num;
	}

	public static int getKbsOne() {
		return kbsOne;
	}

	public static void setKbsOne(int kbsOne) {
		StationDownConfig.kbsOne = kbsOne;
	}

	public static int getJudgeOne() {
		return judgeOne;
	}

	public static void setJudgeOne(int judgeOne) {
		StationDownConfig.judgeOne = judgeOne;
	}

	public static int getKbsTwo() {
		return kbsTwo;
	}

	public static void setKbsTwo(int kbsTwo) {
		StationDownConfig.kbsTwo = kbsTwo;
	}

	public static int getJudgeTwo() {
		return judgeTwo;
	}

	public static void setJudgeTwo(int judgeTwo) {
		StationDownConfig.judgeTwo = judgeTwo;
	}

	public static int getKbsThree() {
		return kbsThree;
	}

	public static void setKbsThree(int kbsThree) {
		StationDownConfig.kbsThree = kbsThree;
	}

	public static int getJudgeThree() {
		return judgeThree;
	}

	public static void setJudgeThree(int judgeThree) {
		StationDownConfig.judgeThree = judgeThree;
	}

	public static int getKbsFour() {
		return kbsFour;
	}

	public static void setKbsFour(int kbsFour) {
		StationDownConfig.kbsFour = kbsFour;
	}

	public static int getJudgeFour() {
		return judgeFour;
	}

	public static void setJudgeFour(int judgeFour) {
		StationDownConfig.judgeFour = judgeFour;
	}

	public static int getKbsFive() {
		return kbsFive;
	}

	public static void setKbsFive(int kbsFive) {
		StationDownConfig.kbsFive = kbsFive;
	}

	public static int getJudgeFive() {
		return judgeFive;
	}

	public static void setJudgeFive(int judgeFive) {
		StationDownConfig.judgeFive = judgeFive;
	}

	public static int getKbsSix() {
		return kbsSix;
	}

	public static void setKbsSix(int kbsSix) {
		StationDownConfig.kbsSix = kbsSix;
	}

	public static int getJudgeSix() {
		return judgeSix;
	}

	public static void setJudgeSix(int judgeSix) {
		StationDownConfig.judgeSix = judgeSix;
	}

	public static int getJudgeSeven() {
		return judgeSeven;
	}

	public static void setJudgeSeven(int judgeSeven) {
		StationDownConfig.judgeSeven = judgeSeven;
	}
}
