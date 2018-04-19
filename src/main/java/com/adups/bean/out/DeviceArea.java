package com.adups.bean.out;

import com.adups.base.AbstractSchema;

/**
 * @author allen
 * Created by allen on 01/08/2017.
 */
public class DeviceArea extends AbstractSchema {

	private String productId;

	private String country;

	private String province;

	private Long newNum;

	private Long totalNum;

	private String pt;

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public Long getNewNum() {
		return newNum;
	}

	public void setNewNum(Long newNum) {
		this.newNum = newNum;
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
