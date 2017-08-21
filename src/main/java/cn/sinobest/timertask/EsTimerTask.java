package cn.sinobest.timertask;

import cn.sinobest.base.util.DateUtil;
import cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc.JdbcService;
import cn.sinobest.es.util.ElasticSearchManager;
import cn.sinobest.es.util.IncrementInsertThread;
import cn.sinobest.jzpt.framework.util.ApplicationContextUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.sql.Clob;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * es增量入库定时器
 * @author yjh
 * @date 2017.08.15
 */
@Component("EsTimerTask")
public class EsTimerTask {
    ApplicationContext context = ApplicationContextUtil.getApplicationContext();
    public void execute(){
        System.out.println("增量定时任务启动");
        //获取es客户端
        TransportClient client = ElasticSearchManager.getClient();
        //jdbc
        JdbcService jdbcService = (JdbcService) context.getBean("simpleQueryJdbcService");
        //获取数据库当前时间
        Timestamp now = DateUtil.getDBCurrentTime(jdbcService);
        //获取增量数据源配置
        String configSql = "SELECT * FROM ES_DATASOURCE WHERE ISUSED = '1'";
        List list = jdbcService.queryForList(configSql,new Object[]{},new int[]{});
        for(Object item : list){
            Map itemMap = (Map)item;
            IncrementInsertThread iiThread = new IncrementInsertThread();
            iiThread.setName((String) itemMap.get("ID"));               //线程名
            iiThread.setDatasourceId((String) itemMap.get("ID"));       //数据源id
            iiThread.setIndex((String) itemMap.get("ES_INDEX"));        //索引
            iiThread.setType((String) itemMap.get("ES_TYPE"));          //类型
            iiThread.setJdbcService(jdbcService);
            iiThread.setLastUpdatedTime((Timestamp) itemMap.get("CHECKTIME"));
            iiThread.setTimePoint(now);
            iiThread.setSql((String) itemMap.get("SQL"));
            iiThread.setClient(client);
            //每个配置开启独立的线程执行增量入库
            iiThread.start();
        }
    }
}
