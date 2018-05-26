package com.demo.bean.out;

import com.demo.base.AbstractSchema;

/**
 * @author allen
 * Created by allen on 07/08/2017.
 */
public class ProductDeltaMerge extends AbstractSchema {

	private String  productId;

	private String  deltaId;

	private String originVersion;

	private String targetVersion;

	private Long countCheck;

	private Long countDownload;

	private Long countUpgrade;

	private Long countDownFail;

	private Long countUpgradeFail;

	private String pt;

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

	public String getTargetVersion() {
		return targetVersion;
	}

	public void setTargetVersion(String targetVersion) {
		this.targetVersion = targetVersion;
	}

	public Long getCountCheck() {
		return countCheck;
	}

	public void setCountCheck(Long countCheck) {
		this.countCheck = countCheck;
	}

	public Long getCountDownload() {
		return countDownload;
	}

	public void setCountDownload(Long countDownload) {
		this.countDownload = countDownload;
	}

	public Long getCountUpgrade() {
		return countUpgrade;
	}

	public void setCountUpgrade(Long countUpgrade) {
		this.countUpgrade = countUpgrade;
	}

	public Long getCountDownFail() {
		return countDownFail;
	}

	public void setCountDownFail(Long countDownFail) {
		this.countDownFail = countDownFail;
	}

	public Long getCountUpgradeFail() {
		return countUpgradeFail;
	}

	public void setCountUpgradeFail(Long countUpgradeFail) {
		this.countUpgradeFail = countUpgradeFail;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}
}
