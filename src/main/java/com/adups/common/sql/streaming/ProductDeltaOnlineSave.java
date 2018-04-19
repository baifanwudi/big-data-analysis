package com.adups.common.sql.streaming;

import com.adups.base.AbstractPreStatementBatch;
import scala.Tuple2;
import scala.Tuple5;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @author allen
 * Created by allen on 12/09/2017.
 */
public class ProductDeltaOnlineSave extends AbstractPreStatementBatch<Tuple2<Tuple5, Integer>> {

	@Override
	public void execute(PreparedStatement preparedStatement, Tuple2<Tuple5, Integer> tuple5IntegerTuple2) throws SQLException {
		preparedStatement.setObject(1, tuple5IntegerTuple2._1()._1());
		preparedStatement.setObject(2, tuple5IntegerTuple2._1()._2());
		preparedStatement.setObject(3, tuple5IntegerTuple2._1()._3());
		preparedStatement.setObject(4, tuple5IntegerTuple2._1()._4());
		preparedStatement.setObject(5, tuple5IntegerTuple2._1()._5());
		preparedStatement.setObject(6, tuple5IntegerTuple2._2());
	}
}
