package com.demo.base;

import com.demo.config.DbcpPool;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

/**
 * @author allen
 * Created by allen on 13/09/2017.
 */
public abstract class AbstractStatement<T> {

	public void putDataBatch(Iterator<T> data) throws SQLException {
		Connection connection= DbcpPool.getConnection();
		Statement statement = connection.createStatement();
		while (data.hasNext()) {
			T t = data.next();
			execute(statement,t);
		}
	}

	/**
	 *  spark插入数据
	 * @param statement
	 * @param t
	 * @throws SQLException
	 */
	public abstract  void execute(Statement statement,T t) throws SQLException ;

}
