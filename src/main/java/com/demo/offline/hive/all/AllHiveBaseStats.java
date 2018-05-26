package com.demo.offline.hive.all;

import com.demo.offline.hive.base.OtaAppLogBase;
import com.demo.offline.hive.base.OtaCheckBase;
import com.demo.offline.hive.base.OtaDownloadBase;
import com.demo.offline.hive.base.OtaUpgradeBase;
import com.demo.util.DateUtil;
import java.io.IOException;

/**
 * @author allen
 * Created by allen on 02/08/2017.
 */
public class AllHiveBaseStats {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		OtaCheckBase otaCheckBase=new OtaCheckBase();
		OtaDownloadBase otaDownloadBase=new OtaDownloadBase();
		OtaUpgradeBase otaUpgradeBase=new OtaUpgradeBase();
		OtaAppLogBase otaAppLogBase=new OtaAppLogBase();

		otaCheckBase.runAll(pt);
		otaDownloadBase.runAll(pt);
		otaUpgradeBase.runAll(pt);
		otaAppLogBase.runAll(pt);
	}
}


