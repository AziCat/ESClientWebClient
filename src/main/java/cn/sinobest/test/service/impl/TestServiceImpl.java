package cn.sinobest.test.service.impl;

import cn.sinobest.base.util.DateUtil;
import cn.sinobest.base.util.PropertiesUtil;
import cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc.JdbcService;
import cn.sinobest.es.service.IElasticSearchService;
import cn.sinobest.es.util.ElasticSearchManager;
import cn.sinobest.es.util.FullDoesInsertThread;
import cn.sinobest.jzpt.framework.util.ApplicationContextUtil;
import cn.sinobest.test.service.ITestService;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("test.TestService")
public class TestServiceImpl implements ITestService {
    @Autowired
    IElasticSearchService elasticSearchService;

    public void test() {
        ApplicationContext context = ApplicationContextUtil.getApplicationContext();
        JdbcService jdbcService = (JdbcService) context.getBean("simpleQueryJdbcService");
        Timestamp time = DateUtil.getDBCurrentTime(jdbcService);
        System.out.println(time);
    }

    public void test2() {
        BulkProcessor bulkProcessor = ElasticSearchManager.getBulkProcessor();
        Date startTime = new Date();
        int count = 0;
        try {
            ApplicationContext context = ApplicationContextUtil.getApplicationContext();
            JdbcService jdbcService = (JdbcService) context.getBean("simpleQueryJdbcService");
            String sql = "SELECT * FROM B_ASJ_AJ";
            Connection conn = jdbcService.getDataBaseConnection();
            Statement st = conn.createStatement();
            st.setFetchSize(100);
            ResultSet rs = st.executeQuery(sql);
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
                bulkProcessor.add(new IndexRequest("aj", "b_asj_aj", id).source(map));
                if (0 == count % 1000) {
                    System.out.println("当前处理数据量---" + count);
                }
            }
            ElasticSearchManager.closeBulkProcess();
            rs.close();
            st.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        Date endTime = new Date();
        float useTime = (endTime.getTime() - startTime.getTime()) / 1000f;
        System.out.println("");
        System.out.println("" + (endTime.getTime() - startTime.getTime()));
        System.out.println("数量(条) : " + count);
        System.out.println("耗时(秒) : " + useTime);
        System.out.println("速度 (条/秒): " + (count / useTime));

    }

    @Override
    public void fulldose() {
        ElasticSearchManager.getClient();

        FullDoesInsertThread ajThread = new FullDoesInsertThread();
        ajThread.setElasticSearchService(elasticSearchService);
        ajThread.setIncrease(PropertiesUtil.getAsBoolean("fulldose.properties", "isIncrease"));
        ajThread.setIncreaseSize(PropertiesUtil.getAsInt("fulldose.properties", "increaseSize"));
        ajThread.setOpen(PropertiesUtil.getAsBoolean("fulldose.properties", "isOpen"));
        ajThread.setSql(PropertiesUtil.getAsString("fulldose.properties", "sql_aj"));
        ajThread.setStartTime(PropertiesUtil.getAsString("fulldose.properties", "startTime"));
        ajThread.setIndex(PropertiesUtil.getAsString("fulldose.properties", "index_aj"));
        ajThread.setType(PropertiesUtil.getAsString("fulldose.properties", "type_aj"));
        ajThread.start();

        FullDoesInsertThread ryThread = new FullDoesInsertThread();
        ryThread.setElasticSearchService(elasticSearchService);
        ryThread.setIncrease(PropertiesUtil.getAsBoolean("fulldose.properties", "isIncrease"));
        ryThread.setIncreaseSize(PropertiesUtil.getAsInt("fulldose.properties", "increaseSize"));
        ryThread.setOpen(PropertiesUtil.getAsBoolean("fulldose.properties", "isOpen"));
        ryThread.setSql(PropertiesUtil.getAsString("fulldose.properties", "sql_ry"));
        ryThread.setStartTime(PropertiesUtil.getAsString("fulldose.properties", "startTime"));
        ryThread.setIndex(PropertiesUtil.getAsString("fulldose.properties", "index_ry"));
        ryThread.setType(PropertiesUtil.getAsString("fulldose.properties", "type_ry"));
        ryThread.start();

        FullDoesInsertThread wpThread = new FullDoesInsertThread();
        wpThread.setElasticSearchService(elasticSearchService);
        wpThread.setIncrease(PropertiesUtil.getAsBoolean("fulldose.properties", "isIncrease"));
        wpThread.setIncreaseSize(PropertiesUtil.getAsInt("fulldose.properties", "increaseSize"));
        wpThread.setOpen(PropertiesUtil.getAsBoolean("fulldose.properties", "isOpen"));
        wpThread.setSql(PropertiesUtil.getAsString("fulldose.properties", "sql_wp"));
        wpThread.setStartTime(PropertiesUtil.getAsString("fulldose.properties", "startTime"));
        wpThread.setIndex(PropertiesUtil.getAsString("fulldose.properties", "index_wp"));
        wpThread.setType(PropertiesUtil.getAsString("fulldose.properties", "type_wp"));
        wpThread.start();

        FullDoesInsertThread dwThread = new FullDoesInsertThread();
        dwThread.setElasticSearchService(elasticSearchService);
        dwThread.setIncrease(PropertiesUtil.getAsBoolean("fulldose.properties", "isIncrease"));
        dwThread.setIncreaseSize(PropertiesUtil.getAsInt("fulldose.properties", "increaseSize"));
        dwThread.setOpen(PropertiesUtil.getAsBoolean("fulldose.properties", "isOpen"));
        dwThread.setSql(PropertiesUtil.getAsString("fulldose.properties", "sql_dw"));
        dwThread.setStartTime(PropertiesUtil.getAsString("fulldose.properties", "startTime"));
        dwThread.setIndex(PropertiesUtil.getAsString("fulldose.properties", "index_dw"));
        dwThread.setType(PropertiesUtil.getAsString("fulldose.properties", "type_dw"));
        dwThread.start();

        FullDoesInsertThread jqThread = new FullDoesInsertThread();
        jqThread.setElasticSearchService(elasticSearchService);
        jqThread.setIncrease(PropertiesUtil.getAsBoolean("fulldose.properties", "isIncrease"));
        jqThread.setIncreaseSize(PropertiesUtil.getAsInt("fulldose.properties", "increaseSize"));
        jqThread.setOpen(PropertiesUtil.getAsBoolean("fulldose.properties", "isOpen"));
        jqThread.setSql(PropertiesUtil.getAsString("fulldose.properties", "sql_jq"));
        jqThread.setStartTime(PropertiesUtil.getAsString("fulldose.properties", "startTime"));
        jqThread.setIndex(PropertiesUtil.getAsString("fulldose.properties", "index_jq"));
        jqThread.setType(PropertiesUtil.getAsString("fulldose.properties", "type_jq"));
        jqThread.start();
    }

    public void test1() {
        Date startTime = new Date();
        int start = 0;
        int end = 1000;
        int size = 1000;
        int count = 0;
        String sql = "SELECT * FROM (SELECT AJ.*,ROWNUM RN FROM (SELECT * FROM B_ASJ_AJ) AJ WHERE ROWNUM <= ?) WHERE RN > ?";
        ApplicationContext context = ApplicationContextUtil.getApplicationContext();
        JdbcService jdbcService = (JdbcService) context.getBean("simpleQueryJdbcService");
        List resultList;
        do {
            resultList = jdbcService.queryForList(sql, new Object[]{end, start}, new int[]{Types.INTEGER, Types.INTEGER});
            Map<String, Map> resultMap = new HashMap<>();
            for (Object aResultList : resultList) {
                Map item = (Map) aResultList;
                resultMap.put((String) item.get("SYSTEMID"), item);
            }
            elasticSearchService.sendBulkProcessRequest(resultMap, "aj", "b_asj_aj");
            start += size;
            end += size;
            count += resultList.size();
            System.out.println("当前处理数据量---" + count);
        } while (resultList.size() > 0);
        Date endTime = new Date();
        float useTime = (endTime.getTime() - startTime.getTime()) / 1000f;
        System.out.println("");
        System.out.println("" + (endTime.getTime() - startTime.getTime()));
        System.out.println("数量(条) : " + count);
        System.out.println("耗时(秒) : " + useTime);
        System.out.println("速度 (条/秒): " + (count / useTime));
    }
}
