package com.adups.offline.save.all;

import com.adups.offline.save.base.*;
import com.adups.offline.save.device.StatsDeviceArea;
import com.adups.offline.save.stats.StatsProductDeltaMerge;
import com.adups.offline.save.stats.StatsProductRegionMerge;
import com.adups.offline.save.stats.StatsProductTotalMerge;
import com.adups.util.DateUtil;

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
