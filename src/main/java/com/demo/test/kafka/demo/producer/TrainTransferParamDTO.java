package com.demo.test.kafka.demo.producer;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author hw41838
 * @Date 2017/5/23 13:35
 * @Email hw41838@ly.com
 */
public class TrainTransferParamDTO {
    @JSONField(ordinal = 1)
    private String partnerId;
    @JSONField(ordinal = 2)
    private String from;//出发站点（城市）
    @JSONField(ordinal = 3)
    private String to;//目的站点（城市）
    @JSONField(ordinal = 4)
    private String date;//出发日期，例：20170517
    @JSONField(ordinal = 6)
    private List<String> transfer_type;//中转方案类型,例：TT火火、TB火汽、TF火空、TS火船
    @JSONField(ordinal = 7)
    private List<String> directs;//直达路线,例：[T,B,F,S]，T火车B汽车F飞机S轮船（不传不返回直达）
    @JSONField(ordinal = 9)
    private boolean isTicket;//是（true）否（false）需要余票信息
    @JSONField(ordinal = 12)
    private String startDate;//请求时间戳
    @JSONField(ordinal = 10)
    private String memberId;
    @JSONField(ordinal = 11)
    private String openId;
    @JSONField(ordinal = 8)
    private String usingCar_Info;//用车信息
    @JSONField(ordinal = 5)
    private String brief;//临时为了入口增加
    @JSONField(ordinal = 13)
    private int transfer_size;//中转方案返回数量
    @JSONField(ordinal = 14)
    private String sign;
    @JSONField(ordinal = 15)
    private String timestamp;
    @JSONField(ordinal = 16)
    private boolean useGZip;
    @JSONField(ordinal = 17)
    private boolean allTicket;//?
    @JSONField(ordinal = 18)
    private List<String> planType = new ArrayList<>();
    @JSONField(ordinal = 19)
    private String method;
    @JSONField(ordinal = 20)
    private String refId;
    @JSONField(ordinal = 21)
    private String fromDataType;//出发参数类型
    @JSONField(ordinal = 22)
    private String toDataType;//到达参数类型
    @JSONField(ordinal = 23)
    private String fromId;//出发参数id
    @JSONField(ordinal = 24)
    private String toId;//到达参数id
    @JSONField(ordinal = 25)
    private String fromStationType;//出发站点类型
    @JSONField(ordinal = 26)
    private String toStationType;//到达站点类型


    public String getRefId() {
        return refId;
    }

    public void setRefId(String refId) {
        this.refId = refId;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<String> getTransfer_type() {
        return transfer_type;
    }

    public void setTransfer_type(List<String> transfer_type) {
        this.transfer_type = transfer_type;
    }

    public List<String> getDirects() {
        return directs;
    }

    public void setDirects(List<String> directs) {
        this.directs = directs;
    }

    public boolean getIsTicket() {
        return isTicket;
    }

    public void setIsTicket(boolean ticket) {
        isTicket = ticket;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public String getOpenId() {
        return openId;
    }

    public void setOpenId(String openId) {
        this.openId = openId;
    }

    public String getUsingCar_Info() {
        return usingCar_Info;
    }

    public void setUsingCar_Info(String usingCar_Info) {
        this.usingCar_Info = usingCar_Info;
    }

    public String getBrief() {
        return brief;
    }

    public void setBrief(String brief) {
        this.brief = brief;
    }

    public int getTransfer_size() {
        return transfer_size;
    }

    public void setTransfer_size(int transfer_size) {
        this.transfer_size = transfer_size;
    }

    public String getSign() {
        return sign;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }

    public String getPartnerId() {
        return partnerId;
    }

    public void setPartnerId(String partnerId) {
        this.partnerId = partnerId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public boolean getUseGZip() {
        return useGZip;
    }

    public void setUseGZip(boolean useGZip) {
        this.useGZip = useGZip;
    }

    public boolean getAllTicket() {
        return allTicket;
    }

    public void setAllTicket(boolean allTicket) {
        this.allTicket = allTicket;
    }

    public List<String> getPlanType() {
        return planType;
    }

    public void setPlanType(List<String> planType) {
        this.planType = planType;
    }

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

    public String getFromDataType() {
        return fromDataType;
    }

    public void setFromDataType(String fromDataType) {
        this.fromDataType = fromDataType;
    }

    public String getToDataType() {
        return toDataType;
    }

    public void setToDataType(String toDataType) {
        this.toDataType = toDataType;
    }

    public String getFromId() {
        return fromId;
    }

    public void setFromId(String fromId) {
        this.fromId = fromId;
    }

    public String getToId() {
        return toId;
    }

    public void setToId(String toId) {
        this.toId = toId;
    }

    public String getFromStationType() {
        return fromStationType;
    }

    public void setFromStationType(String fromStationType) {
        this.fromStationType = fromStationType;
    }

    public String getToStationType() {
        return toStationType;
    }

    public void setToStationType(String toStationType) {
        this.toStationType = toStationType;
    }

    @Override
    public String toString() {
        return "TrainTransferParamDTO{" +
                "partnerId='" + partnerId + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", date='" + date + '\'' +
                ", transfer_type=" + transfer_type +
                ", directs=" + directs +
                ", isTicket=" + isTicket +
                ", startDate='" + startDate + '\'' +
                ", memberId='" + memberId + '\'' +
                ", openId='" + openId + '\'' +
                ", usingCar_Info='" + usingCar_Info + '\'' +
                ", brief='" + brief + '\'' +
                ", transfer_size=" + transfer_size +
                ", sign='" + sign + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", useGZip=" + useGZip +
                ", allTicket=" + allTicket +
                ", planType=" + planType +
                ", method='" + method + '\'' +
                ", refId='" + refId + '\'' +
                ", fromDataType='" + fromDataType + '\'' +
                ", toDataType='" + toDataType + '\'' +
                ", fromId='" + fromId + '\'' +
                ", toId='" + toId + '\'' +
                ", fromStationType='" + fromStationType + '\'' +
                ", toStationType='" + toStationType + '\'' +
                '}';
    }
}
