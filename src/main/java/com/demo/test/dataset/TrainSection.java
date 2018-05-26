package com.demo.test.dataset;


import com.demo.base.AbstractSchema;

public class TrainSection extends AbstractSchema {

    private String trainNode;

    private String transportCode;

    private String durationDate;

    private Integer beginId;

    private String beginStationCode;

    private String beginStationName;

    private String beginTime;

    private Integer endId;

    private String endStationCode;

    private String endStationName;

    private String endTime;

    public String getTrainNode() {
        return trainNode;
    }

    public void setTrainNode(String trainNode) {
        this.trainNode = trainNode;
    }

    public String getTransportCode() {
        return transportCode;
    }

    public void setTransportCode(String transportCode) {
        this.transportCode = transportCode;
    }

    public String getDurationDate() {
        return durationDate;
    }

    public void setDurationDate(String durationDate) {
        this.durationDate = durationDate;
    }

    public Integer getBeginId() {
        return beginId;
    }

    public void setBeginId(Integer beginId) {
        this.beginId = beginId;
    }

    public String getBeginStationCode() {
        return beginStationCode;
    }

    public void setBeginStationCode(String beginStationCode) {
        this.beginStationCode = beginStationCode;
    }

    public String getBeginStationName() {
        return beginStationName;
    }

    public void setBeginStationName(String beginStationName) {
        this.beginStationName = beginStationName;
    }

    public String getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(String beginTime) {
        this.beginTime = beginTime;
    }

    public Integer getEndId() {
        return endId;
    }

    public void setEndId(Integer endId) {
        this.endId = endId;
    }

    public String getEndStationCode() {
        return endStationCode;
    }

    public void setEndStationCode(String endStationCode) {
        this.endStationCode = endStationCode;
    }

    public String getEndStationName() {
        return endStationName;
    }

    public void setEndStationName(String endStationName) {
        this.endStationName = endStationName;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

}
