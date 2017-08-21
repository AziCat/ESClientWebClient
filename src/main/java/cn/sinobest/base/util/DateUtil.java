package cn.sinobest.base.util;

import cn.sinobest.dzzw.ga.framework.query.core.engine.jdbc.JdbcService;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 时间相关操作工具类
 * @author yanjunhao
 *
 */
public class DateUtil {
	/**
	 * 获取两个日期之间的日期（不包含开始和结束时间）
	 * @param start 开始日期
	 * @param end 结束日期
	 * @return 日期集合
	 */
	public static List<Date> getBetweenDates(Date start, Date end) {
	    List<Date> result = new ArrayList<>();
	    Calendar tempStart = Calendar.getInstance();
	    tempStart.setTime(start);
	    tempStart.add(Calendar.DAY_OF_YEAR, 1);

	    Calendar tempEnd = Calendar.getInstance();
	    tempEnd.setTime(end);
	    while (tempStart.before(tempEnd)) {
	        result.add(tempStart.getTime());
	        tempStart.add(Calendar.DAY_OF_YEAR, 1);
	    }
	    return result;
	}
	/**
	 * 字符转日期
	 * @param strDate 字符日期
	 * @param format 格式
	 * @return 日期
	 * @throws ParseException 转换异常
	 */
	public static Date strToDate(String strDate,String format) throws ParseException{

		if(null != strDate && null != format && !"".equals(strDate) && !"".equals(format)){
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return sdf.parse(strDate);
		}
		return null;
	}
	/**
	 * 日期转字符
	 * @param date 日期
	 * @param format 格式
	 * @return 字符日期
	 */
	public static String dateToStr(Date date,String format){
		if(null != date && null != format &&  !"".equals(format)){
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return sdf.format(date);
		}
		return null;
	}

	/**
	 * 获取数据库当前时间
	 * @param jdbcService jdbc服务实例
	 * @return 数据库当前时间
	 */
	public static Timestamp getDBCurrentTime(JdbcService jdbcService){
		String sql = "SELECT SYSDATE AS TIME FROM DUAL";
		if(null != jdbcService){
			Map result = jdbcService.queryForSingle(sql,new Object[]{},new int[]{});
			return (Timestamp) result.get("TIME");
		}
		return null;
	}
	/*public static void main(String[] args) {
		String format = "yyyy-MM-dd";
		String startTimeStr = "2017-05-30";
		String endTimeStr = "2017-06-03";
		Date start;
		Date end;
		try {
			start = strToDate(startTimeStr, format);
			end = strToDate(endTimeStr, format);
			List<Date> dates = getBetweenDates(start, end);
			for(Date d : dates){
				System.out.println(dateToStr(d, format));
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}*/
}