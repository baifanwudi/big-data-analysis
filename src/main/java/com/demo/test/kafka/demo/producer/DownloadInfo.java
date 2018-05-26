package com.demo.test.kafka.demo.producer;

import com.demo.base.AbstractSchema;

/**
 * @company adups
 * @author wangxiaojing
 * @date 2017年7月24日
 **/
public class DownloadInfo extends AbstractSchema {
    private String downStart;
    private String downEnd;
    private Integer downloadStatus;
    private String apnType;
    private String extStr;
    private Integer downSize;
    private String downIp;
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

	public String getDownStart() {
		return downStart;
	}

	public void setDownStart(String downStart) {
		this.downStart = downStart;
	}

	public String getDownEnd() {
		return downEnd;
	}

	public void setDownEnd(String downEnd) {
		this.downEnd = downEnd;
	}

	public Integer getDownloadStatus() {
		return downloadStatus;
	}

	public void setDownloadStatus(Integer downloadStatus) {
		this.downloadStatus = downloadStatus;
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

	public Integer getDownSize() {
		return downSize;
	}

	public void setDownSize(Integer downSize) {
		this.downSize = downSize;
	}

	public String getDownIp() {
		return downIp;
	}

	public void setDownIp(String downIp) {
		this.downIp = downIp;
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
		final StringBuilder sb = new StringBuilder("DownloadInfo{");
		sb.append("downStart='").append(downStart).append('\'');
		sb.append(", downEnd='").append(downEnd).append('\'');
		sb.append(", downloadStatus=").append(downloadStatus);
		sb.append(", apnType='").append(apnType).append('\'');
		sb.append(", extStr='").append(extStr).append('\'');
		sb.append(", downSize=").append(downSize);
		sb.append(", downIp='").append(downIp).append('\'');
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


//	public StructType produceSchema(){
//
//		List<StructField> inputFields=new ArrayList<>();
//
//		String stringType="downStart,downEnd,apnType,extStr,downIp,mid,deviceId,originVersion,nowVersion,ip,province,city";
//
//		String timeType="createTime";
//
//		String integerType="downloadStatus,downSize";
//
//		String longStype="productId,deltaId";
//
//		for(String stringTmp:stringType.split(",")){
//			inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
//		}
//
//		inputFields.add(DataTypes.createStructField(timeType,DataTypes.TimestampType,true));
//
//		for(String integerTmp:integerType.split(",")){
//			inputFields.add(DataTypes.createStructField(integerTmp,DataTypes.IntegerType,true));
//		}
//
//		for(String longTmp:longStype.split(",")){
//			inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,true));
//		}
//
//		return DataTypes.createStructType(inputFields);
//	}

}
