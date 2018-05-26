package com.demo.bean.out;

import com.demo.base.AbstractSchema;

/**
 * @author allen
 * @date 06/03/2018.
 */
public class AppCountVersion extends AbstractSchema {


	private Long productId;

	private String packageName;

	private String packageVersion;

	private Long totalNum;

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getPackageVersion() {
		return packageVersion;
	}

	public void setPackageVersion(String packageVersion) {
		this.packageVersion = packageVersion;
	}

	public Long getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(Long totalNum) {
		this.totalNum = totalNum;
	}
}
