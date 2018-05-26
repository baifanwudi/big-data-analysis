package com.demo.common.sql.streaming;

import com.demo.base.AbstractStatement;
import com.demo.bean.out.DownRate;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author allen
 * Created on 05/12/2017.
 */
public class CheckDownRate extends AbstractStatement<DownRate> {

	@Override
	public void execute(Statement statement, DownRate downRate) throws SQLException {
		String sql = "insert into download_rate_product(product_id,lac,cid,down_rate_sum," +
				"count_sum,down_rate,create_time,rate_time)value(" +
				"" + downRate.getProductId() + "," +
				"'" + downRate.getLac() + "'," +
				"'" + downRate.getCid() + "'," +
				"" + downRate.getDownRateSum() + "," +
				"" + downRate.getCountNum() + "," +
				"" + downRate.getDownRate() + "," +
				"'" + downRate.getCreateTime() + "'," +
				"'" + downRate.getRateTime() + "')";
		statement.executeUpdate(sql);
	}
}
