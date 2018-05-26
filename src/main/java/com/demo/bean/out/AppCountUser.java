package com.demo.bean.out;

import com.demo.base.AbstractSchema;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountUser extends AbstractSchema {

	private String productId;

	private String packageName;

	private Long newNum;

	private Long activeNum;

	private String pt;

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public Long getNewNum() {
		return newNum;
	}

	public void setNewNum(Long newNum) {
		this.newNum = newNum;
	}

	public Long getActiveNum() {
		return activeNum;
	}

	public void setActiveNum(Long activeNum) {
		this.activeNum = activeNum;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}
}
