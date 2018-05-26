package com.demo.bean.out;

import com.demo.base.AbstractSchema;

/**
 * @author allen
 */
public class UpgradeBase extends AbstractSchema {

	private String updateStatus;

	private String apnType;

	private String extStr;

	private String mid;

	private String deviceId;

	private String  productId;

	private String deltaId;

	private String originVersion;

	private String nowVersion;

	private String createTime;

	private String ip;

	private String province;

	private String city;

	private String dataType;

	private String pt;

	public String getUpdateStatus() {
		return updateStatus;
	}

	public void setUpdateStatus(String updateStatus) {
		this.updateStatus = updateStatus;
	}

	public String getApnType() {
		return apnType;
	}

	public void setApnType(String apnType) {
		this.apnType = apnType;
	}

	public String getExtStr() {
		return extStr;
	}

	public void setExtStr(String extStr) {
		this.extStr = extStr;
	}

	public String getMid() {
		return mid;
	}

	public void setMid(String mid) {
		this.mid = mid;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getDeltaId() {
		return deltaId;
	}

	public void setDeltaId(String deltaId) {
		this.deltaId = deltaId;
	}

	public String getOriginVersion() {
		return originVersion;
	}

	public void setOriginVersion(String originVersion) {
		this.originVersion = originVersion;
	}

	public String getNowVersion() {
		return nowVersion;
	}

	public void setNowVersion(String nowVersion) {
		this.nowVersion = nowVersion;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}
}
