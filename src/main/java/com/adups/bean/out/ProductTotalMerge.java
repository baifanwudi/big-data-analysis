package com.adups.bean.out;

import com.adups.base.AbstractSchema;

/**
 * @author allen
 * Created by allen on 09/08/2017.
 */
public class ProductTotalMerge extends AbstractSchema {

	private String productId;

	private long activeNum;

	private long newNum;

	private long totalNum;

	private long countCheck;

	private long countDownload;

	private long countUpgrade;

	private long countDownFail;

	private long countUpgradeFail;

	private String pt;

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public long getActiveNum() {
		return activeNum;
	}

	public void setActiveNum(long activeNum) {
		this.activeNum = activeNum;
	}

	public long getNewNum() {
		return newNum;
	}

	public void setNewNum(long newNum) {
		this.newNum = newNum;
	}

	public long getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(long totalNum) {
		this.totalNum = totalNum;
	}

	public long getCountCheck() {
		return countCheck;
	}

	public void setCountCheck(long countCheck) {
		this.countCheck = countCheck;
	}

	public long getCountDownload() {
		return countDownload;
	}

	public void setCountDownload(long countDownload) {
		this.countDownload = countDownload;
	}

	public long getCountUpgrade() {
		return countUpgrade;
	}

	public void setCountUpgrade(long countUpgrade) {
		this.countUpgrade = countUpgrade;
	}

	public long getCountDownFail() {
		return countDownFail;
	}

	public void setCountDownFail(long countDownFail) {
		this.countDownFail = countDownFail;
	}

	public long getCountUpgradeFail() {
		return countUpgradeFail;
	}

	public void setCountUpgradeFail(long countUpgradeFail) {
		this.countUpgradeFail = countUpgradeFail;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}
}
