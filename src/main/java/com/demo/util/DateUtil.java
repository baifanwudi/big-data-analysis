package com.demo.util;

import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author allen
 * Created by allen on 25/07/2017.
 */
public final class DateUtil {

	private final static String PREFIX = "pt=";

	private final static String TIME_PATTERN = "yyyy-MM-dd";

	private static final String TIME_SDF = "yyyy-MM-dd HH:mm:ss";

	public static String producePtOrYesterday(String[] args) {
		return args.length < 1 ? nowAfterOrBeforeDay(-1) : args[0];
	}

	private static Date parseStringToDate(String pt) {
		try {
			return DateUtils.parseDate(pt, TIME_PATTERN);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String nowPtDay() {
		return nowAfterOrBeforeDay(0);
	}

	private static String nowAfterOrBeforeDay(int day) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, day);
		return new SimpleDateFormat(TIME_PATTERN).format(calendar.getTime());
	}

	public static String ptAfterOrBeforeDay(String pt, int day) {
		Date date = parseStringToDate(pt);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, day);
		return new SimpleDateFormat(TIME_PATTERN).format(calendar.getTime());

	}

	public static String pathNowPtWithPre() {
		return pathPtWithPre(nowAfterOrBeforeDay(0));
	}

	public static String pathPtWithPre(String pt) {
		return pathPtWithPre(PREFIX, pt);
	}

	public static String pathPtWithPre(String prefix, String pt) {
		return prefix + pt;
	}

	public static String getMinuteTimeYmd() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.MINUTE, -5);
		Date date = calendar.getTime();
		return new SimpleDateFormat(TIME_SDF).format(date);
	}

}
