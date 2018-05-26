package com.demo.base;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import java.io.Serializable;

/**
 * @author allen
 * spark 所有类型必须继承 Serializable
 * produceBeanEncoder相当于 Encoder<T> tmp= Encoders.bean(T.class);
 */
public abstract class AbstractSchema<T> implements Serializable {

	@SuppressWarnings("unchecked")
	public Encoder<T> produceBeanEncoder(){
		return (Encoder<T>) Encoders.bean(this.getClass());
	}
}

