package com.adups.test.bak.logfilter;

import com.adups.util.DateUtil;

import java.io.IOException;

/**
 * @author allen on 16/08/2017.
 */
public class LogFilterAll {
	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);
		DeviceInfoFilter deviceInfoFilter =new DeviceInfoFilter();
		OtaAppLogFilter otaAppLogFilter =new OtaAppLogFilter();
		OtaCheckLogFilter otaCheckLogFilter =new OtaCheckLogFilter();
		OtaDownloadLogFilter otaDownloadLogFilter =new OtaDownloadLogFilter();
		OtaUpgradeLogFilter otaUpgradeLogFilter =new OtaUpgradeLogFilter();

		deviceInfoFilter.runAll(pt);
		otaAppLogFilter.runAll(pt);
		otaCheckLogFilter.runAll(pt);
		otaDownloadLogFilter.runAll(pt);
		otaUpgradeLogFilter.runAll(pt);

	}
}
