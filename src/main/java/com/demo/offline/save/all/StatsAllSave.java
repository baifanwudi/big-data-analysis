package com.demo.offline.save.all;

import com.demo.offline.save.base.*;
import com.demo.offline.save.device.StatsDeviceArea;
import com.demo.offline.save.stats.StatsProductDeltaMerge;
import com.demo.offline.save.stats.StatsProductRegionMerge;
import com.demo.offline.save.stats.StatsProductTotalMerge;
import com.demo.util.DateUtil;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 09/08/2017.
 */
public class StatsAllSave {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		StatsCheckBase statsCheckBase=new StatsCheckBase();
		StatsDownBase statsDownBase=new StatsDownBase();
		StatsUpgradeBase statsUpgradeBase=new StatsUpgradeBase();
		StatsProductDeltaMerge statsProductDeltaMerge =new StatsProductDeltaMerge();
		StatsDeviceArea statsDeviceArea=new StatsDeviceArea();
		StatsProductRegionMerge statsProductRegionMerge=new StatsProductRegionMerge();
		StatsProductTotalMerge statsProductTotalMerge=new StatsProductTotalMerge();

		statsCheckBase.runAll(pt);
		statsDownBase.runAll(pt);
		statsUpgradeBase.runAll(pt);
		statsProductDeltaMerge.runAll(pt);
		statsDeviceArea.runAll(pt);
		statsProductRegionMerge.runAll(pt);
		statsProductTotalMerge.runAll(pt);
	}

}
