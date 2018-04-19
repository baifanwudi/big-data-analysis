package com.adups.common.sql.device;

import com.adups.base.AbstractPreStatementBatch;
import com.adups.bean.out.DeviceArea;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class DeviceAreaSave extends AbstractPreStatementBatch<DeviceArea> {

	@Override
	public void execute(PreparedStatement statement, DeviceArea deviceArea) throws SQLException {
		statement.setObject(1, deviceArea.getProductId());
		statement.setObject(2, deviceArea.getCountry());
		statement.setObject(3, deviceArea.getProvince());
		statement.setObject(4, deviceArea.getNewNum());
		statement.setObject(5, deviceArea.getTotalNum());
		statement.setObject(6, deviceArea.getPt());
	}
}
