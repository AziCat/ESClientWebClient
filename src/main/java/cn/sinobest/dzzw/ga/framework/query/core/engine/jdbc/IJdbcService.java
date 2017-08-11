package cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 *
 * @project simplequery
 * @version 1.0
 * @author huangshaohua
 * @date  2013-5-11 下午5:42:36
 */
public interface IJdbcService {

	public JdbcTemplate getJdbcTemplate();

	/**
	 * 分页查询
	 */
	public List pagedQuery(String sql, Object[] args, int[] argsType, int pageNum, int pageSize);

	/**
	 * 支持大对象更新
	 */
	public int update(String sql, Object[] args, int[] argsType);

	/**
	 * 对于分页传入的是当前页的最开始数据量，以及结束数据量
	 *
	 * @param sql
	 * @param args
	 * @param argsType
	 * @param startIndex
	 * @param lastIndex
	 * @return
	 */
	public List queryPage(String sql, Object[] args, int[] argsType, int startIndex, int lastIndex);

	/**
	 * 用于返回所有数据量的操作
	 *
	 * @param sql
	 *            ：sql语句
	 * @param args
	 *            ：参数
	 * @param argsType
	 *            ：参数类型
	 * @return
	 */
	public int getCntQuery(String sql, Object[] args, int[] argsType);

	/**
	 * 找单个对象
	 */
	public Map queryForSingle(String sql, Object[] args, int[] argsType);

	/**
	 * 按列表返回查找结果
	 */
	public List queryForList(String sql, Object[] args, int[] argsType);


	public Connection getDataBaseConnection() ;

}
