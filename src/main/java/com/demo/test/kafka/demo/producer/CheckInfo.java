package com.demo.test.kafka.demo.producer;

import com.demo.base.AbstractSchema;

public class CheckInfo extends AbstractSchema {
	private String networkType;
	private String lac;
	private String cid;
	private String mcc;
	private String mnc;
	private String rxlev;
	private String mid;
	private String deviceId;
	private Long productId;
	private Long deltaId;
	private String originVersion;
	private String nowVersion;
	private String createTime;
	private String ip;
	private String province;
	private String city;

	public String getNetworkType() {
		return networkType;
	}

	public void setNetworkType(String networkType) {
		this.networkType = networkType;
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

	public String getMcc() {
		return mcc;
	}

	public void setMcc(String mcc) {
		this.mcc = mcc;
	}

	public String getMnc() {
		return mnc;
	}

	public void setMnc(String mnc) {
		this.mnc = mnc;
	}

	public String getRxlev() {
		return rxlev;
	}

	public void setRxlev(String rxlev) {
		this.rxlev = rxlev;
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

	public Long getProductId() {
		return productId;
	}

	public void setProductId(Long productId) {
		this.productId = productId;
	}

	public Long getDeltaId() {
		return deltaId;
	}

	public void setDeltaId(Long deltaId) {
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

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("CheckInfo{");
		sb.append("networkType='").append(networkType).append('\'');
		sb.append(", lac='").append(lac).append('\'');
		sb.append(", cid='").append(cid).append('\'');
		sb.append(", mcc='").append(mcc).append('\'');
		sb.append(", mnc='").append(mnc).append('\'');
		sb.append(", rxlev='").append(rxlev).append('\'');
		sb.append(", mid='").append(mid).append('\'');
		sb.append(", deviceId='").append(deviceId).append('\'');
		sb.append(", productId=").append(productId);
		sb.append(", deltaId=").append(deltaId);
		sb.append(", originVersion='").append(originVersion).append('\'');
		sb.append(", nowVersion='").append(nowVersion).append('\'');
		sb.append(", createTime='").append(createTime).append('\'');
		sb.append(", ip='").append(ip).append('\'');
		sb.append(", province='").append(province).append('\'');
		sb.append(", city='").append(city).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
