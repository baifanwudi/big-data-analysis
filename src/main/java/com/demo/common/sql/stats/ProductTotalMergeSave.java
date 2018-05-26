package com.demo.common.sql.stats;

import com.demo.base.AbstractPreStatementBatch;
import com.demo.bean.out.ProductTotalMerge;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class ProductTotalMergeSave extends AbstractPreStatementBatch<ProductTotalMerge> {

	@Override
	public void execute(PreparedStatement statement, ProductTotalMerge productTotalMerge) throws SQLException {
		statement.setObject(1, productTotalMerge.getProductId());
		statement.setObject(2, productTotalMerge.getActiveNum());
		statement.setObject(3, productTotalMerge.getNewNum());
		statement.setObject(4, productTotalMerge.getTotalNum());
		statement.setObject(5, productTotalMerge.getCountCheck());
		statement.setObject(6, productTotalMerge.getCountDownload());
		statement.setObject(7, productTotalMerge.getCountUpgrade());
		statement.setObject(8, productTotalMerge.getCountDownFail());
		statement.setObject(9, productTotalMerge.getCountUpgradeFail());
		statement.setObject(10,productTotalMerge.getPt());
	}
}
