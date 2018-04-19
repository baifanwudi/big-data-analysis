package com.adups.common.sql.app;

import com.adups.base.AbstractPreStatementBatch;
import com.adups.bean.out.AppCountUser;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountUserSave extends AbstractPreStatementBatch<AppCountUser> {

	@Override
	public void execute(PreparedStatement statement, AppCountUser appCountUser) throws SQLException {
		statement.setObject(1, appCountUser.getProductId());
		statement.setObject(2, appCountUser.getPackageName());
		statement.setObject(3, appCountUser.getNewNum());
		statement.setObject(4, appCountUser.getActiveNum());
		statement.setObject(5, appCountUser.getPt());
	}
}
