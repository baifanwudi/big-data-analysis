package com.adups.online.all;

import com.adups.online.flume.DeviceAreaOnline;
import com.adups.online.flume.ProductDeltaMergeOnline;
import com.adups.online.flume.ProductTotalMergeOnline;
import com.adups.util.DateUtil;

import java.io.IOException;

/**
 * @author allen
 * Created by allen on 14/08/2017.
 */
public class StatsOnlineAll {

	public static void main(String[] args) throws IOException {
		String pt = DateUtil.producePtOrYesterday(args);

		DeviceAreaOnline deviceAreaOnline=new DeviceAreaOnline();
		ProductDeltaMergeOnline productDeltaMergeOnline=new ProductDeltaMergeOnline();
		ProductTotalMergeOnline productTotalMergeOnline=new ProductTotalMergeOnline();

		deviceAreaOnline.runAll(pt,false);
		productDeltaMergeOnline.runAll(pt,false);
		productTotalMergeOnline.runAll(pt,false);

	}
}
