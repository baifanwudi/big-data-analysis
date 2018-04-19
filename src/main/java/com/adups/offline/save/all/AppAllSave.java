package com.adups.offline.save.all;

import com.adups.offline.save.app.AppCountAreaData;
import com.adups.offline.save.app.AppCountUpgradeData;
import com.adups.offline.save.app.AppCountUserData;
import com.adups.offline.save.app.AppCountVersionData;
import com.adups.util.DateUtil;

import java.io.IOException;

/**
 * @author allen
 * @date 06/03/2018.
 */
public class AppAllSave {

	public static void main(String[] args) throws IOException {

		String pt = DateUtil.producePtOrYesterday(args);

		AppCountAreaData appCountAreaData=new AppCountAreaData();
		AppCountUpgradeData appCountUpgradeData=new AppCountUpgradeData();
		AppCountUserData appCountUserData=new AppCountUserData();
//		AppCountVersionData appCountVersionData=new AppCountVersionData();

		appCountAreaData.runAll(pt);
		appCountUpgradeData.runAll(pt);
		appCountUserData.runAll(pt);
//		appCountVersionData.runAll(pt);
	}
}
