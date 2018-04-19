package com.adups.base;

import com.adups.config.DbcpPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * @author allen
 * Created by allen on 08/09/2017.
 */

public abstract class AbstractPreStatementBatch<T> {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public void putDataBatch(Iterator<T> data, String sql) {
		Connection connection;
		PreparedStatement statement;
		int i=0;
		int batchSize=1000;
		try {
			connection= DbcpPool.getConnection();
			connection.setAutoCommit(false);
			statement = connection.prepareStatement(sql);
			while (data.hasNext()) {
				i++;
				T t = data.next();
				execute(statement, t);
				statement.addBatch();
				if(i%batchSize==0){
					statement.executeBatch();
					connection.commit();
				}
			}
			statement.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * spark PreparedStatement 插入数据
	 * @param preparedStatement
	 * @param t
	 * @throws SQLException
	 */
	public abstract void execute(PreparedStatement preparedStatement, T t) throws SQLException;

}
