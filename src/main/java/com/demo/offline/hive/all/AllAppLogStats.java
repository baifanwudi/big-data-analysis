package com.demo.offline.hive.all;

import com.demo.offline.hive.app.*;
import com.demo.util.DateUtil;

import java.io.IOException;

/**
 * @author allen
 * @date 06/03/2018.
 */
public class AllAppLogStats {

	public static void main(String[] args) throws IOException {

		String pt = DateUtil.producePtOrYesterday(args);

		AppCheckAll appCheckAll=new AppCheckAll();
		AppCheckLog appCheckLog=new AppCheckLog();
		AppCheckNew appCheckNew=new AppCheckNew();
		AppCheckReport appCheckReport=new AppCheckReport();
		AppRegisterInfo appRegisterInfo=new AppRegisterInfo();

		appCheckAll.runAll(pt);
		appCheckLog.runAll(pt);
		appCheckNew.runAll(pt);
		appCheckReport.runAll(pt);
		appRegisterInfo.runAll(pt);
	}
}
