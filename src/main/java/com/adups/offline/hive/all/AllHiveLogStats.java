package com.adups.offline.hive.all;

import com.adups.offline.hive.log.*;
import com.adups.util.DateUtil;
import java.io.IOException;

/**
 * @author allen
 * Created by allen on 27/07/2017.
 */
public class AllHiveLogStats {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		OtaAppLog otaAppLog=new OtaAppLog();
		OtaCheckLog otaCheckLog =new OtaCheckLog();
		OtaDownloadLog otaDownload=new OtaDownloadLog();
		OtaUpgradeLog otaUpgrade=new OtaUpgradeLog();
		DeviceInfo deviceInfo=new DeviceInfo();

		otaAppLog.runAll(pt);
		otaCheckLog.runAll(pt);
		otaDownload.runAll(pt);
		otaUpgrade.runAll(pt);
		deviceInfo.runAll(pt);
	}
}
