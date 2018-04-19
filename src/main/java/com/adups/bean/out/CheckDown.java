package com.adups.bean.out;

import java.io.Serializable;

/**
 * @author allen
 * Created on 05/12/2017.
 */
public class CheckDown implements Serializable {

	private Long productId;

	private String deviceId;

	private String lac;

	private String cid;

	private Float downRate;

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getLac() {
		return lac;
	}

	public void setLac(String lac) {
		this.lac = lac;
	}

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}

	public Float getDownRate() {
		return downRate;
	}

	public void setDownRate(Float downRate) {
		this.downRate = downRate;
	}
}
