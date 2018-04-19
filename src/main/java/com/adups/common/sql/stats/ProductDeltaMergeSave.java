package com.adups.common.sql.stats;

import com.adups.base.AbstractStatement;
import com.adups.bean.out.ProductDeltaMerge;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author allen
 *         Created by allen on 13/09/2017.
 */
public class ProductDeltaMergeSave extends AbstractStatement<ProductDeltaMerge> {

	@Override
	public void execute(Statement statement, ProductDeltaMerge productDeltaMerge) throws SQLException {
		String insertSql = "insert into stats_product_delta_merge (product_id,delta_id,origin_version,target_version,count_check," +
				"count_download,count_upgrade,count_downfail,count_upgradefail,pt) values (" + productDeltaMerge.getProductId() + "," +
				productDeltaMerge.getDeltaId() + ",'" + productDeltaMerge.getOriginVersion() + "','" + productDeltaMerge.getTargetVersion() + "',"
				+ productDeltaMerge.getCountCheck() + "," + productDeltaMerge.getCountDownload() + "," + productDeltaMerge.getCountUpgrade() + ","
				+ productDeltaMerge.getCountDownFail() + "," + productDeltaMerge.getCountUpgradeFail() + ",'" + productDeltaMerge.getPt() + "')";
		statement.executeUpdate(insertSql);
	}
}
