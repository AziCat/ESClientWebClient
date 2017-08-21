package cn.sinobest.simplequery.domain;


import java.util.List;

public class ResultDto {
    private String successful;
    private String msg;
    private long total;
    private float max_score;
    private List<Source> sources;

    public String getSuccessful() {
        return successful;
    }

    public void setSuccessful(String successful) {
        this.successful = successful;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public float getMax_score() {
        return max_score;
    }

    public void setMax_score(float max_score) {
        this.max_score = max_score;
    }

    public List<Source> getSources() {
        return sources;
    }

    public void setSources(List<Source> sources) {
        this.sources = sources;
    }

    @Override
    public String toString() {
        return "ResultDto{" +
                "successful='" + successful + '\'' +
                ", msg='" + msg + '\'' +
                ", total=" + total +
                ", max_score=" + max_score +
                ", sources=" + sources +
                '}';
    }
}
