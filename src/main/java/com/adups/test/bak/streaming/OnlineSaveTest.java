package com.adups.test.bak.streaming;


import com.adups.test.bak.base.StreamJdbcBase;

import java.sql.SQLException;

/**
 * Created by allen on 11/09/2017.
 */
public class OnlineSaveTest extends StreamJdbcBase<OnlineBean> {

	@Override
	public void process(OnlineBean value) {
		try {
			statement.executeUpdate("insert into stats_online_check_down_upgrade(product_id,delta_id,origin_version," +
					"target_version,type,num,create_time) values ("+value.getProductId()+","+value.getDeltaId()+",'"
					+value.getOriginVersion()+"','"+value.getNowVersion()+"','"+value.getType()+"',"+value.getNum()+",'"+
					value.getCreateTime()+ "')");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
