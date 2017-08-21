package cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import cn.sinobest.dzzw.ga.framework.query.web.WebContextHolderUtil;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.servlet.ServletContext;

import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * @author huangshaohua
 * @version 1.0
 * @project simplequery
 * @date 2013-5-11 下午5:42:44
 */
public class JdbcService extends JdbcDaoSupport implements IJdbcService {
    private LobHandler lobHandler;
    private static ApplicationContext ctx;

    /**
     * 分页查询
     */
    public List pagedQuery(String sql, Object[] args, int[] argsType, int pageNum, int pageSize) {
        if (0 >= pageNum) {
            throw new RuntimeException("开始页数pageNum 应该是从1开始的。.");
        }
        int startIndex = (pageNum - 1) * pageSize;
        int lastIndex = startIndex + pageSize;
        StringBuffer paginationSQL = new StringBuffer(" SELECT * FROM ( ");
        paginationSQL.append(" SELECT temp.* ,ROWNUM num FROM ( ");
        paginationSQL.append(sql);
        paginationSQL.append("　) temp where ROWNUM <= " + lastIndex);
        paginationSQL.append(" ) WHERE num >= " + startIndex);
        return getJdbcTemplate().queryForList(paginationSQL.toString(), args, argsType);
    }

    /**
     * 支持大对象更新
     */
    public int update(String sql, Object[] args, int[] argsType) {
        return getJdbcTemplate().update(sql, new LobCreatorPreparedStatementSetter(
                args, argsType, lobHandler.getLobCreator()));
    }

    /**
     * 重写上方法，对于分页传入的是当前页的最开始数据量，以及结束数据量
     *
     * @param sql
     * @param args
     * @param argsType
     * @param startIndex
     * @param lastIndex
     * @return
     */
    public List queryPage(String sql, Object[] args, int[] argsType,
                          int startIndex, int lastIndex) {
        StringBuffer paginationSQL = new StringBuffer(" SELECT * FROM ( ");
        paginationSQL.append(" SELECT temp.* ,ROWNUM num FROM ( ");
        paginationSQL.append(sql);
        paginationSQL.append("　) temp where ROWNUM <= " + lastIndex);
        paginationSQL.append(" ) WHERE num >= " + startIndex);
        return getJdbcTemplate().queryForList(paginationSQL.toString(), args,
                argsType);
    }


    /**
     * 用于返回所有数据量的操作
     *
     * @param sql：sql语句
     * @param args：参数
     * @param argsType：参数类型
     * @return
     */
    public int getCntQuery(String sql, Object[] args, int[] argsType) {
        if (logger.isDebugEnabled()) {
            System.out.println("执行统计分析-开始");
            System.out.println("统计分析脚本： " + sql);
            if (null == args || args.length == 0) {
                System.out.println("sql参数个数为0");
            } else {
                for (int i = 0; i < args.length; i++) {
                    System.out.println("sql参数" + i + ":" + args[i]);
                }
            }
            if (null == argsType || argsType.length == 0) {
                System.out.println("sql参数个数为0");
            } else {
                for (int i = 0; i < argsType.length; i++) {
                    System.out.println("sql参数类型" + i + ":" + argsType[i]);
                }
            }
        }
        return getJdbcTemplate().queryForInt(sql, args, argsType);
    }

    /**
     * 找单个对象
     */
    public Map queryForSingle(String sql, Object[] args, int[] argsType) {
        Map map = null;
        List list = getJdbcTemplate().queryForList(sql, args, argsType);
        if (list.size() > 0) {
            map = (Map) list.get(0);
        }
        return map;
    }

    /**
     * 按列表返回查找结果
     */
    public List queryForList(String sql, Object[] args, int[] argsType) {
        return getJdbcTemplate().queryForList(sql, args, argsType);
    }

    public LobHandler getLobHandler() {
        return lobHandler;
    }

    public void setLobHandler(LobHandler lobHandler) {
        this.lobHandler = lobHandler;
    }

    public Connection getDataBaseConnection() {
        return this.getConnection();
    }

    public static IJdbcService getInstance(String name) {
        Object object = getBean(name);
        return (IJdbcService) object;
    }

    /**
     * @param name
     * @return
     */
    public static Object getBean(String name) {
        Object bean = null;
        try {
            bean = getApplicationContext().getBean(name);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("检查下Spring的配置。Spring容器中没有找到相应的bean。 " + name);
        }
        return bean;
    }

    /**
     * @return
     */
    public static ApplicationContext getApplicationContext() {

        if (null == ctx) {
            ServletContext servletContext = WebContextHolderUtil
                    .getServletContext();
            if (null != servletContext) {
                ctx = WebApplicationContextUtils
                        .getWebApplicationContext(servletContext);

            } /*else {
                ctx = new ClassPathXmlApplicationContext(CONTEXT_PATHS);
			}*/

        }
        return ctx;
    }

    public String ClobToString(Clob clob) throws Exception{
        String reString = "";
        Reader is = clob.getCharacterStream();// 得到流
        BufferedReader br = new BufferedReader(is);
        String s = br.readLine();
        StringBuffer sb = new StringBuffer();
        while (s != null) {// 执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
            sb.append(s);
            s = br.readLine();
        }
        reString = sb.toString();
        if (br != null) {
            br.close();
        }
        if (is != null) {
            is.close();
        }
        return reString;
    }
}
