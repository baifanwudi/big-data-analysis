package com.demo.common.sql.logfilter;

import com.demo.base.AbstractPreStatementBatch;
import com.demo.bean.out.CheckBase;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class CheckSave extends AbstractPreStatementBatch<CheckBase> {

	@Override
	public void execute(PreparedStatement statement, CheckBase checkBase) throws SQLException {
		statement.setObject(1, checkBase.getMid());
		statement.setObject(2, checkBase.getDeviceId());
		statement.setObject(3, checkBase.getProductId());
		statement.setObject(4, checkBase.getDeltaId());
		statement.setObject(5, checkBase.getOriginVersion());
		statement.setObject(6, checkBase.getNowVersion());
		statement.setObject(7, checkBase.getCreateTime());
		statement.setObject(8, checkBase.getIp());
		statement.setObject(9, checkBase.getProvince());
		statement.setObject(10, checkBase.getCity());
		statement.setObject(11, checkBase.getNetworkType());
		statement.setObject(12, checkBase.getLac());
		statement.setObject(13, checkBase.getCid());
		statement.setObject(14, checkBase.getMcc());
		statement.setObject(15, checkBase.getMnc());
		statement.setObject(16, checkBase.getRxlev());
		statement.setObject(17, checkBase.getDataType());
		statement.setObject(18, checkBase.getPt());
	}
}
