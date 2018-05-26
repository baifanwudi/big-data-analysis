package com.demo.offline.hive.all;

import com.demo.offline.hive.device.OffDeviceAreaStats;
import com.demo.offline.hive.device.OffDeviceRegionTotalStats;
import com.demo.util.DateUtil;
import java.io.IOException;

/**
 * @author allen
 * @date 14/11/2017.
 */
public class AllDeviceStats {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		OffDeviceAreaStats offDeviceAreaStats=new OffDeviceAreaStats();
		OffDeviceRegionTotalStats offDeviceRegionTotalStats=new OffDeviceRegionTotalStats();

		offDeviceAreaStats.runAll(pt);
		offDeviceRegionTotalStats.runAll(pt);
	}
}
