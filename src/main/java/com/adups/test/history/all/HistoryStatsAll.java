package com.adups.test.history.all;

import com.adups.test.history.*;
import com.adups.util.DateUtil;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 27/09/2017.
 */
public class HistoryStatsAll {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		HistoryOtaCheckBase historyOtaCheckBase=new HistoryOtaCheckBase();
		HistoryOtaDownloadBase historyOtaDownloadBase=new HistoryOtaDownloadBase();
		HistoryOtaUpgradeBase historyOtaUpgradeBase=new HistoryOtaUpgradeBase();

		HistoryStatsCheckBase historyStatsCheckBase=new HistoryStatsCheckBase();
		HistoryStatsDownBase historyStatsDownBase=new HistoryStatsDownBase();
		HistoryStatsUpgradeBase historyStatsUpgradeBase=new HistoryStatsUpgradeBase();

		HistoryStatsProductDeltaMerge historyStatsProductDeltaMerge=new HistoryStatsProductDeltaMerge();

		historyOtaCheckBase.runAll(pt);
		historyOtaDownloadBase.runAll(pt);
		historyOtaUpgradeBase.runAll(pt);

		historyStatsCheckBase.runAll(pt);
		historyStatsDownBase.runAll(pt);
		historyStatsUpgradeBase.runAll(pt);
		historyStatsProductDeltaMerge.runAll(pt);
	}
}
