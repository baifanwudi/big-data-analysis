package com.demo.common.sql.flume;

import com.demo.base.AbstractPreStatementBatch;
import com.demo.bean.out.DeviceArea;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 08/09/2017.
 */
public class DeviceAreaOnlineSave extends AbstractPreStatementBatch<DeviceArea> {

	@Override
	public void execute(PreparedStatement preparedStatement, DeviceArea deviceArea) throws SQLException {
		preparedStatement.setObject(1, deviceArea.getProductId());
		preparedStatement.setObject(2, deviceArea.getCountry());
		preparedStatement.setObject(3, deviceArea.getProvince());
		preparedStatement.setObject(4, deviceArea.getNewNum());
		preparedStatement.setObject(5, deviceArea.getTotalNum());
		preparedStatement.setObject(6, deviceArea.getPt());
		preparedStatement.setObject(7,deviceArea.getNewNum());
		preparedStatement.setObject(8,deviceArea.getTotalNum());
	}
}
