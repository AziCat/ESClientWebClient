package cn.sinobest.timertask;

import cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc.JdbcService;
import cn.sinobest.jzpt.framework.util.ApplicationContextUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component("EsTimerTask")
public class EsTimerTask {
    ApplicationContext context = ApplicationContextUtil.getApplicationContext();
    public void execute(){
        JdbcService jdbcService = (JdbcService) context.getBean("simpleQueryJdbcService");
        System.out.println(jdbcService+"--"+new Date().toString());
    }
}
