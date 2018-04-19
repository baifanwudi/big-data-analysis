package com.adups.bean.out;

import com.adups.base.AbstractSchema;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountUpgrade extends AbstractSchema {

	private String productId;

	private String packageName;

	private Long checkNum;

	private Long downNum;

	private Long upNum;

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

	public Long getCheckNum() {
		return checkNum;
	}

	public void setCheckNum(Long checkNum) {
		this.checkNum = checkNum;
	}

	public Long getDownNum() {
		return downNum;
	}

	public void setDownNum(Long downNum) {
		this.downNum = downNum;
	}

	public Long getUpNum() {
		return upNum;
	}

	public void setUpNum(Long upNum) {
		this.upNum = upNum;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}
}
