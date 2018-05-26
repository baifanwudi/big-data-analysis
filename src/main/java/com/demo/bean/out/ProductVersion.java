package com.demo.bean.out;


import com.demo.base.AbstractSchema;

/**
 * @author allen
 * Created by allen on 28/07/2017.
 */
public class ProductVersion extends AbstractSchema {

	private String version ;

	private long num;

	private long productId;

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public long getNum() {
		return num;
	}

	public void setNum(long num) {
		this.num = num;
	}

	public long getProductId() {
		return productId;
	}

	public void setProductId(long productId) {
		this.productId = productId;
	}
}
