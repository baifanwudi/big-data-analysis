package com.adups.common.sql.flume;

import com.adups.base.AbstractPreStatementBatch;
import com.adups.bean.out.ProductTotalMerge;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 09/09/2017.
 */
public class ProductTotalMergeOnlineSave extends AbstractPreStatementBatch<ProductTotalMerge> {

	@Override
	public void execute(PreparedStatement preparedStatement, ProductTotalMerge productTotalMerge) throws SQLException {
		preparedStatement.setObject(1, productTotalMerge.getProductId());
		preparedStatement.setObject(2, productTotalMerge.getActiveNum());
		preparedStatement.setObject(3, productTotalMerge.getNewNum());
		preparedStatement.setObject(4, productTotalMerge.getTotalNum());
		preparedStatement.setObject(5, productTotalMerge.getCountCheck());
		preparedStatement.setObject(6, productTotalMerge.getCountDownload());
		preparedStatement.setObject(7, productTotalMerge.getCountUpgrade());
		preparedStatement.setObject(8, productTotalMerge.getCountDownFail());
		preparedStatement.setObject(9, productTotalMerge.getCountUpgradeFail());
		preparedStatement.setObject(10, productTotalMerge.getPt());
		preparedStatement.setObject(11, productTotalMerge.getActiveNum());
		preparedStatement.setObject(12, productTotalMerge.getNewNum());
		preparedStatement.setObject(13, productTotalMerge.getTotalNum());
		preparedStatement.setObject(14, productTotalMerge.getCountCheck());
		preparedStatement.setObject(15, productTotalMerge.getCountDownload());
		preparedStatement.setObject(16, productTotalMerge.getCountUpgrade());
		preparedStatement.setObject(17, productTotalMerge.getCountDownFail());
		preparedStatement.setObject(18, productTotalMerge.getCountUpgradeFail());
	}
}
