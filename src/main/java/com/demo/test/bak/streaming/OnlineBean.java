package com.demo.test.bak.streaming;

import com.demo.base.AbstractSchema;

import java.sql.Timestamp;

/**
 * Created by allen on 11/09/2017.
 */
public class OnlineBean  extends AbstractSchema {
	private Long productId;

	private Long  deltaId;

	private String originVersion;

	private String nowVersion;

	private Long  num ;

	private String type;

	private Timestamp createTime;

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

	public Long getNum() {
		return num;
	}

	public void setNum(Long num) {
		this.num = num;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Timestamp getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Timestamp createTime) {
		this.createTime = createTime;
	}
}
