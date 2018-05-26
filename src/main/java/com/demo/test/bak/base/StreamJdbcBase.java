package com.demo.test.bak.base;

import com.demo.config.DbcpPool;
import org.apache.spark.sql.ForeachWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by allen on 08/09/2017.
 */
public abstract class StreamJdbcBase<T>extends ForeachWriter<T> {

	public Connection connection=null;

	public Statement statement=null;

	@Override
	public boolean open(long partitionId, long version) {

		connection=DbcpPool.getConnection();

		try {
			statement = connection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public void close(Throwable errorOrNull) {
		try {
			if(connection!=null) {
				connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
