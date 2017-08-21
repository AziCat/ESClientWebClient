package cn.sinobest.es.util;

import cn.sinobest.base.util.DateUtil;
import cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc.JdbcService;
import cn.sinobest.es.service.IElasticSearchService;
import cn.sinobest.jzpt.framework.util.ApplicationContextUtil;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.springframework.context.ApplicationContext;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 全量入库
 *
 * @author yjh
 * @date 2017年8月11日
 */
public class FullDoesInsertThread extends Thread{

    private IElasticSearchService elasticSearchService;
    private boolean isOpen;
    private String sql;
    private boolean isIncrease;
    private int increaseSize;
    private String startTime;
    private String index;
    private String type;

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

    public IElasticSearchService getElasticSearchService() {
        return elasticSearchService;
    }

    public void setElasticSearchService(IElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }


    public boolean isOpen() {
        return isOpen;
    }

    public void setOpen(boolean open) {
        isOpen = open;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isIncrease() {
        return isIncrease;
    }

    public void setIncrease(boolean increase) {
        isIncrease = increase;
    }

    public int getIncreaseSize() {
        return increaseSize;
    }

    public void setIncreaseSize(int increaseSize) {
        this.increaseSize = increaseSize;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }
    @Override
    public void run() {
        if (isOpen) {
            BulkProcessor bulkProcessor = ElasticSearchManager.getBulkProcessor();
            ApplicationContext context = ApplicationContextUtil.getApplicationContext();
            JdbcService jdbcService = (JdbcService) context.getBean("simpleQueryJdbcService");
            long startTimeLong = System.currentTimeMillis();
            int count = 0;
            try {
                Date queryStartTime = DateUtil.strToDate(startTime, "yyyy-MM-dd");
                Calendar c = Calendar.getInstance();
                c.setTime(queryStartTime);
                c.add(Calendar.DATE, increaseSize);
                Date queryEndTime = c.getTime();


                Connection conn = jdbcService.getDataBaseConnection();
                PreparedStatement pst = conn.prepareStatement(sql);
                pst.setFetchSize(200);
                ResultSet rs = null;
                while (queryStartTime.getTime() <= startTimeLong) {
                    pst.setDate(1, new java.sql.Date(queryStartTime.getTime()));
                    pst.setDate(2, new java.sql.Date(queryEndTime.getTime()));
                    rs = pst.executeQuery();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (rs.next()) {
                        count++;
                        Map map = new HashMap();
                        String id = rs.getString("SYSTEMID");
                        for (int i = 1; i <= columnCount; i++) {
                            Object object = rs.getObject(i);
                            String columnName = metaData.getColumnName(i);
                            map.put(columnName, object);
                        }
                        //获取b_asj_aj表的id
                        String aj_id = (String)map.get("AJ_SYSTEMID");
                        if(null != aj_id && !"".equals(aj_id)){
                            //如果关联了案件表，索引的id为组合id
                            id += "-"+aj_id;
                        }
                        bulkProcessor.add(new IndexRequest(index, type, id).source(map));
                    }
                    System.out.println(index+"-"+DateUtil.dateToStr(queryStartTime,"yyyy-MM-dd")+"~"+DateUtil.dateToStr(queryEndTime,"yyyy-MM-dd")
                            +"处理数据量---" + count);

                    //增加天数
                    queryStartTime = queryEndTime;
                    c.setTime(queryEndTime);
                    c.add(Calendar.DATE, increaseSize);
                    queryEndTime = c.getTime();
                }

                //ElasticSearchManager.closeBulkProcess();
                if (null != rs) {
                    rs.close();
                }
                pst.close();
                conn.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            long endTimeLong = System.currentTimeMillis();
            float useTime = (endTimeLong - startTimeLong) / 1000f;
            System.out.println("---------------------------"+index+"----------------------------");
            System.out.println("" + (endTimeLong - startTimeLong));
            System.out.println("数量(条) : " + count);
            System.out.println("耗时(秒) : " + useTime);
            System.out.println("速度 (条/秒): " + (count / useTime));
            System.out.println("---------------------------"+index+"----------------------------");
        }
    }

}
