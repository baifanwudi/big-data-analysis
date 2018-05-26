package com.demo.common.sql.stats;

import com.demo.base.AbstractPreStatementBatch;
import com.demo.bean.out.ProductRegionMerge;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class ProductRegionMergeSave extends AbstractPreStatementBatch<ProductRegionMerge> {

	@Override
	public void execute(PreparedStatement statement, ProductRegionMerge productRegionMerge) throws SQLException {
		statement.setObject(1, productRegionMerge.getProductId());
		statement.setObject(2, productRegionMerge.getActiveNum());
		statement.setObject(3, productRegionMerge.getNewNum());
		statement.setObject(4, productRegionMerge.getTotalNum());
		statement.setObject(5, productRegionMerge.getCountCheck());
		statement.setObject(6, productRegionMerge.getCountDownload());
		statement.setObject(7, productRegionMerge.getCountUpgrade());
		statement.setObject(8, productRegionMerge.getCountDownFail());
		statement.setObject(9, productRegionMerge.getCountUpgradeFail());
		statement.setObject(10,productRegionMerge.getDataType());
		statement.setObject(11,productRegionMerge.getPt());
	}
}
