package com.demo.bean.out;

import com.demo.base.AbstractSchema;

/**
 * @author allen
 * @date 06/11/2017.
 */
public class DeviceVersion extends AbstractSchema {

    private String productId;

	private String deviceId ;

	private String mid;

	private String version;

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}
}
