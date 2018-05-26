package com.demo.online.all;

import com.demo.online.flume.DeviceAreaOnline;
import com.demo.online.flume.ProductDeltaMergeOnline;
import com.demo.online.flume.ProductTotalMergeOnline;
import com.demo.util.DateUtil;

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
