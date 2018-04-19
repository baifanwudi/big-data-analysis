package com.adups.common.sql.app;

import com.adups.base.AbstractPreStatementBatch;
import com.adups.bean.out.AppCountVersion;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * @date 06/03/2018.
 */
public class AppCountVersionSave extends AbstractPreStatementBatch<AppCountVersion> {
	@Override
	public void execute(PreparedStatement statement, AppCountVersion appCountVersion) throws SQLException {
		statement.setObject(1, appCountVersion.getProductId());
		statement.setObject(2, appCountVersion.getPackageName());
		statement.setObject(3, appCountVersion.getPackageVersion());
		statement.setObject(4, appCountVersion.getTotalNum());
	}
}
