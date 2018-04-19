package com.adups.common.sql.logfilter;

import com.adups.base.AbstractPreStatementBatch;
import com.adups.bean.out.UpgradeBase;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class UpgradeSave extends AbstractPreStatementBatch<UpgradeBase> {

	@Override
	public void execute(PreparedStatement statement, UpgradeBase upgradeBase) throws SQLException {
		statement.setObject(1, upgradeBase.getMid());
		statement.setObject(2, upgradeBase.getDeviceId());
		statement.setObject(3, upgradeBase.getProductId());
		statement.setObject(4, upgradeBase.getOriginVersion());
		statement.setObject(5, upgradeBase.getNowVersion());
		statement.setObject(6, upgradeBase.getUpdateStatus());
		statement.setObject(7, upgradeBase.getDeltaId());
		statement.setObject(8, upgradeBase.getIp());
		statement.setObject(9, upgradeBase.getProvince());
		statement.setObject(10, upgradeBase.getApnType());
		statement.setObject(11, upgradeBase.getCity());
		statement.setObject(12, upgradeBase.getExtStr());
		statement.setObject(13, upgradeBase.getCreateTime());
		statement.setObject(14, upgradeBase.getDataType());
		statement.setObject(15, upgradeBase.getPt());
	}
}
