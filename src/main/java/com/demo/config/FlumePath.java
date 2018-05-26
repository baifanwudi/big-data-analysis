package com.demo.config;

/**
 * @author allen
 * Created by allen on 21/08/2017.
 */
public class FlumePath {

	private static final String PREFIX_PATH= PropertiesConfig.FLUME_PATH;

	public static final String CHECK_PATH=PREFIX_PATH+"ota_check/";

	public static final String DOWN_PATH=PREFIX_PATH+"ota_download/";

	public static  final String UPGRADE_PATH=PREFIX_PATH+"ota_upgrade/";

	public static final String APP_LOG_PATH=PREFIX_PATH+"ota_app_log/";

	public static final String DEVICE_INFO_PATH=PREFIX_PATH+"device_info/";

	public static final String APP_CHECK_ALL_PATH=PREFIX_PATH+"iot_all_app/";

	public static final String APP_CHECK_LOG_PATH=PREFIX_PATH+"iot_app_log/";

	public static final String APP_CHECK_NEW_PATH=PREFIX_PATH+"iot_new_app/";

	public static final String APP_CHECK_REPORT_PATH=PREFIX_PATH+"iot_report_app/";

	public static final String APP_REGISTER_INFO_PATH=PREFIX_PATH+"iot_app_user_all/";
}
