package com.adups.bean.out;

import com.adups.base.AbstractSchema;

/**
 * @author  allen
 */
public class DownRate extends AbstractSchema {

    private Long productId;

    private String lac;

    private String cid;

    private Double downRateSum;

    private Long countNum;

    private Double downRate;

    private String createTime;

    private String rateTime;

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
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

    public Double getDownRateSum() {
        return downRateSum;
    }

    public void setDownRateSum(Double downRateSum) {
        this.downRateSum = downRateSum;
    }

    public Long getCountNum() {
        return countNum;
    }

    public void setCountNum(Long countNum) {
        this.countNum = countNum;
    }

    public Double getDownRate() {
        return downRate;
    }

    public void setDownRate(Double downRate) {
        this.downRate = downRate;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getRateTime() {
        return rateTime;
    }

    public void setRateTime(String rateTime) {
        this.rateTime = rateTime;
    }
}
