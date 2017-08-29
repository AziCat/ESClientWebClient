package cn.sinobest.simplequery.domain;

import java.util.Arrays;
import java.util.List;

/**
 * 通用查询实体
 * @author yjh
 */
public class SimpleParam {
    private String userid;
    private String deptCode;
    private String queryStr;
    private String queryType;
    private long startTime;
    private long endTime;
    private String index;
    private String type;
    private int from;
    private int size;
    private String order;
    private String orderType;
    private String field;
    private String highLight;

    public String getHighLight() {
        return highLight;
    }

    public void setHighLight(String highLight) {
        this.highLight = highLight;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getDeptCode() {
        return deptCode;
    }

    public void setDeptCode(String deptCode) {
        this.deptCode = deptCode;
    }

    public String getQueryStr() {
        return queryStr;
    }

    public void setQueryStr(String queryStr) {
        this.queryStr = queryStr;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public static List<String> getNotNullList(){
        String[] list = new String[]{"userid","deptCode","queryStr","index","type","from","size"};
        return Arrays.asList(list);
    }
}