package cn.sinobest.simplequery.domain;

import java.util.List;
import java.util.Map;

/**
 * 高级查询参数实体
 * @author yjh
 */
public class SeniorParam extends SimpleParam{

    private Map<String,String[]> must;
    private Map<String,String[]> should;
    private Map<String,String[]> mustNot;

    public Map<String, String[]> getMust() {
        return must;
    }

    public void setMust(Map<String, String[]> must) {
        this.must = must;
    }

    public Map<String, String[]> getShould() {
        return should;
    }

    public void setShould(Map<String, String[]> should) {
        this.should = should;
    }

    public Map<String, String[]> getMustNot() {
        return mustNot;
    }

    public void setMustNot(Map<String, String[]> mustNot) {
        this.mustNot = mustNot;
    }

    public static List<String> getNotNullList() {
        return SimpleParam.getNotNullList();
    }
}
