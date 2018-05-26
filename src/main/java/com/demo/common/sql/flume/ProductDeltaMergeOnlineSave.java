package com.demo.common.sql.flume;

import com.demo.base.AbstractStatement;
import com.demo.bean.out.ProductDeltaMerge;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class ProductDeltaMergeOnlineSave extends AbstractStatement<ProductDeltaMerge> {

	@Override
	public void execute(Statement statement, ProductDeltaMerge productDeltaMerge)throws SQLException {
		String insertOrUpdateSql="INSERT INTO stats_product_delta_merge (product_id,delta_id,origin_version,target_version," +
				"count_check,count_download,count_downfail,count_upgrade,count_upgradefail,pt)" +
				" VALUES ('"+productDeltaMerge.getProductId()+"','"+productDeltaMerge.getDeltaId()+"','"+productDeltaMerge.getOriginVersion()+"','"+productDeltaMerge.getTargetVersion()+"',"+productDeltaMerge.getCountCheck()+","+
				productDeltaMerge.getCountDownload()+","+productDeltaMerge.getCountDownFail()+","+productDeltaMerge.getCountUpgrade()+","+productDeltaMerge.getCountUpgradeFail()+",'"+productDeltaMerge.getPt()+"') " +
				"ON DUPLICATE KEY UPDATE count_check="+productDeltaMerge.getCountCheck()+",count_download="+productDeltaMerge.getCountDownload()+",count_downfail="+productDeltaMerge.getCountDownFail()+"," +
				"count_upgradefail="+productDeltaMerge.getCountUpgradeFail()+",count_upgrade="+productDeltaMerge.getCountUpgrade();
		statement.executeUpdate(insertOrUpdateSql);
	}
}
