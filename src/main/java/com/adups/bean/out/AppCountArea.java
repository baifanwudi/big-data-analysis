package com.adups.bean.out;

import com.adups.base.AbstractSchema;

/**
 * @author allen
 * @date 28/02/2018.
 */
public class AppCountArea extends AbstractSchema {

	private String productId;

	private String packageName;

	private String countryEn;

	private String countryZh;

	private String provinceEn;

	private String provinceZh;

	private Long totalNum;

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

	public String getCountryEn() {
		return countryEn;
	}

	public void setCountryEn(String countryEn) {
		this.countryEn = countryEn;
	}

	public String getCountryZh() {
		return countryZh;
	}

	public void setCountryZh(String countryZh) {
		this.countryZh = countryZh;
	}

	public String getProvinceEn() {
		return provinceEn;
	}

	public void setProvinceEn(String provinceEn) {
		this.provinceEn = provinceEn;
	}

	public String getProvinceZh() {
		return provinceZh;
	}

	public void setProvinceZh(String provinceZh) {
		this.provinceZh = provinceZh;
	}

	public Long getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(Long totalNum) {
		this.totalNum = totalNum;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}
}
