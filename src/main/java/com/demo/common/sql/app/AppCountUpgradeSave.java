package com.demo.common.sql.app;

import com.demo.base.AbstractPreStatementBatch;
import com.demo.bean.out.AppCountUpgrade;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountUpgradeSave extends AbstractPreStatementBatch<AppCountUpgrade> {

	@Override
	public void execute(PreparedStatement statement, AppCountUpgrade appCountUpgrade) throws SQLException {
		statement.setObject(1, appCountUpgrade.getProductId());
		statement.setObject(2, appCountUpgrade.getPackageName());
		statement.setObject(3, appCountUpgrade.getCheckNum());
		statement.setObject(4, appCountUpgrade.getDownNum());
		statement.setObject(5, appCountUpgrade.getUpNum());
		statement.setObject(6, appCountUpgrade.getPt());
	}
}
