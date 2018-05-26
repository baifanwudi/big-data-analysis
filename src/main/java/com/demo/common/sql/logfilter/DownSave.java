package com.demo.common.sql.logfilter;

import com.demo.base.AbstractPreStatementBatch;
import com.demo.bean.out.DownBase;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class DownSave extends AbstractPreStatementBatch<DownBase> {

	@Override
	public void execute(PreparedStatement statement, DownBase downBase) throws SQLException {
		statement.setObject(1, downBase.getMid());
		statement.setObject(2, downBase.getDeviceId());
		statement.setObject(3, downBase.getProductId());
		statement.setObject(4, downBase.getOriginVersion());
		statement.setObject(5, downBase.getNowVersion());
		statement.setObject(6, downBase.getDownStart());
		statement.setObject(7, downBase.getDownEnd());
		statement.setObject(8, downBase.getDeltaId());
		statement.setObject(9, downBase.getDownloadStatus());
		statement.setObject(10, downBase.getCreateTime());
		statement.setObject(11, downBase.getIp());
		statement.setObject(12, downBase.getProvince());
		statement.setObject(13, downBase.getApnType());
		statement.setObject(14, downBase.getCity());
		statement.setObject(15, downBase.getExtStr());
		statement.setObject(16, downBase.getDownSize());
		statement.setObject(17, downBase.getIp());
		statement.setObject(18, downBase.getDataType());
		statement.setObject(19, downBase.getPt());
	}
}
