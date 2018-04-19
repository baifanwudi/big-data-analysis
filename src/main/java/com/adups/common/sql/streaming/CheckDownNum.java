package com.adups.common.sql.streaming;

import com.adups.base.AbstractStatement;
import com.adups.config.StationDownConfig;
import com.adups.bean.out.DownNum;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author allen
 * Created on 05/12/2017.
 */
public class CheckDownNum extends AbstractStatement<DownNum> {

	@Override
	public void execute(Statement statement, DownNum downNum) throws SQLException {
		Integer num= StationDownConfig.getNum();
		String sql = "insert into down_nums(lac,cid,num)value('" + downNum.getLac() + "','" + downNum.getCid() + "',"
				+ num + " + (" + downNum.getNums() + ")) on duplicate key update num = " + num + " + (" + downNum.getNums() + ")";
		statement.executeUpdate(sql);
	}
}
