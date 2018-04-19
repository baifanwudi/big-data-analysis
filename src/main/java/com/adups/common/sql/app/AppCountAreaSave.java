package com.adups.common.sql.app;

import com.adups.base.AbstractPreStatementBatch;
import com.adups.bean.out.AppCountArea;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountAreaSave extends AbstractPreStatementBatch<AppCountArea> {

	@Override
	public void execute(PreparedStatement statement, AppCountArea appCountArea) throws SQLException {
		statement.setObject(1, appCountArea.getProductId());
		statement.setObject(2, appCountArea.getPackageName());
		statement.setObject(3, appCountArea.getCountryEn());
		statement.setObject(4, appCountArea.getCountryZh());
		statement.setObject(5, appCountArea.getProvinceEn());
		statement.setObject(6, appCountArea.getProvinceZh());
		statement.setObject(7, appCountArea.getTotalNum());
		statement.setObject(8, appCountArea.getPt());
	}
}
