package com.adups.common.sql.device;

import com.adups.base.AbstractStatement;
import com.adups.bean.out.ProductVersion;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author allen
 * @date 02/11/2017.
 */
public class ProductVersionSave extends AbstractStatement<ProductVersion> {

	@Override
	public void execute(Statement statement, ProductVersion productVersion) throws SQLException {
		String insertSql="insert into stats_product_version (product_id,version,versionNum) values ("+productVersion.getProductId()+",'"
				+productVersion.getVersion()+"',"+productVersion.getNum()+")";
		statement.executeUpdate(insertSql);
	}
}
