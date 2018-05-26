package com.demo.bean.out;

import com.demo.base.AbstractSchema;


/**
 * @author allen
 */
public class DownNum extends AbstractSchema {

    private String lac;

    private String cid;

    private Long nums;

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

    public Long getNums() {
        return nums;
    }

    public void setNums(Long nums) {
        this.nums = nums;
    }
}
