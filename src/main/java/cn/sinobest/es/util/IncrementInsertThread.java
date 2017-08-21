package cn.sinobest.es.util;

import cn.sinobest.base.util.DateUtil;
import cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc.JdbcService;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;

import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * 增量入库线程
 *
 * @author yjh
 * @date 2017.08.15
 */
public class IncrementInsertThread extends Thread {
    private String sql;
    private Timestamp timePoint;
    private JdbcService jdbcService;
    private String datasourceId;
    private String index;
    private String type;
    private Timestamp lastUpdatedTime;
    private TransportClient client;

    public TransportClient getClient() {
        return client;
    }

    public void setClient(TransportClient client) {
        this.client = client;
    }

    public Timestamp getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Timestamp lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public String getDatasourceId() {
        return datasourceId;
    }

    public void setDatasourceId(String datasourceId) {
        this.datasourceId = datasourceId;
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

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Timestamp getTimePoint() {
        return timePoint;
    }

    public void setTimePoint(Timestamp timePoint) {
        this.timePoint = timePoint;
    }

    public JdbcService getJdbcService() {
        return jdbcService;
    }

    public void setJdbcService(JdbcService jdbcService) {
        this.jdbcService = jdbcService;
    }

    @Override
    public void run() {
        System.out.println("线程" + this.getName() + "开始执行");
        if (null != sql && !"".equals(sql)) {
            int count = 0;
            Calendar c = Calendar.getInstance();
            c.setTime(lastUpdatedTime);
            //时间延时问题处理，前后时间冗余4分钟的数据
            c.add(Calendar.MINUTE,-4);
            //把sql中的{ST}转成yyyymmddhh24miss的开始时间
            String st = DateUtil.dateToStr(c.getTime(), "yyyyMMddHHmmss");
            //时间延时问题处理，前后时间冗余4分钟的数据
            c.setTime(timePoint);
            c.add(Calendar.MINUTE,4);
            //把sql中的{ET}转成yyyymmddhh24miss的结束时间
            String et = DateUtil.dateToStr(c.getTime(), "yyyyMMddHHmmss");
            sql = sql.replace("{ST}", st).replace("{ET}", et);
            System.out.println(this.getName() + "-->" + sql);
            try {
                //sql执行
                Connection conn = jdbcService.getDataBaseConnection();
                PreparedStatement pst = conn.prepareStatement(sql);
                pst.setFetchSize(200);
                ResultSet rs;
                rs = pst.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                //获取es连接
                BulkRequestBuilder bulkRequest = client.prepareBulk();
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
                    String aj_id = (String) map.get("AJ_SYSTEMID");
                    if (null != aj_id && !"".equals(aj_id)) {
                        //如果关联了案件表，索引的id为组合id
                        id += "-" + aj_id;
                    }
                    bulkRequest.add(client.prepareIndex(index, type, id).setSource(map));
                }
                if (count != 0) {
                    //发送请求
                    BulkResponse bulkResponse = bulkRequest.get();
                    //失败处理
                    if (bulkResponse.hasFailures()) {
                        // process failures by iterating through each bulk response item
                    }
                }
                rs.close();

                //更新数据源的checktime
                String updateSql = "UPDATE ES_DATASOURCE t SET T.CHECKTIME = TO_DATE('" + DateUtil.dateToStr(timePoint, "yyyyMMddHHmmss") + "','YYYYMMDDHH24MISS') WHERE T.ID = '" + datasourceId + "'";
                pst = conn.prepareStatement(updateSql);
                pst.executeUpdate();

                pst.close();
                conn.close();
                System.out.println("线程" + this.getName() + "执行完毕,(" + st + "-" + et + ")数据量(" + count + ")");
            } catch (Exception e) {
                e.printStackTrace();
                //异常处理
            }
        }
    }
}
