package com.adups.common;

import com.adups.config.DbcpPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public class BeforeBatchPut {

	public void executeSqlBeforeBatch(String sql) throws SQLException {

		Connection connection= DbcpPool.getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.execute();
	}
}
