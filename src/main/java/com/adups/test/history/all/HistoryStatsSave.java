package com.adups.test.history.all;

import com.adups.offline.save.device.StatsDeviceArea;
import com.adups.offline.save.stats.StatsProductTotalMerge;
import com.adups.test.bak.device.DeviceAreaOffline;
import com.adups.util.DateUtil;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 09/08/2017.
 */
public class HistoryStatsSave {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		DeviceAreaOffline deviceAreaOffline=new DeviceAreaOffline();
		StatsDeviceArea statsDeviceArea=new StatsDeviceArea();
		StatsProductTotalMerge statsProductTotalMerge=new StatsProductTotalMerge();

		deviceAreaOffline.runAll(pt);
		statsDeviceArea.runAll(pt);
		statsProductTotalMerge.runAll(pt);

	}

}
