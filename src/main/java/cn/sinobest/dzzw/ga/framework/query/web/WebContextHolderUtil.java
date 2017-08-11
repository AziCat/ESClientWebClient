/**
 * web上下文ThreadLocal持有类，用于在任意JAVABEAN中获取到web上下文有关对象
 */
package cn.sinobest.dzzw.ga.framework.query.web;

import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;


/**
 * @author huangjiajun
 *
 */
public final class WebContextHolderUtil {
	public static final String DYNAMIC_PARAMS_HOLDER_QUERYENTITY_KEY = "queryEntity";
	public static final String DYNAMIC_PARAMS_HOLDER_QUERYVIEW_KEY = "queryView";
	public static final String SIMPLEQUERY_START_TIME = "simplequery_start_time";
	public static final String SIMPLEQUERY_DATABASE_QUERY_START = "SIMPLEQUERY_DATABASE_QUERY_START";
	public static final String SIMPLEQUERY_DATABASE_QUERY_END = "SIMPLEQUERY_DATABASE_QUERY_END";
	public static final String REQUEST_CURRENTPAGE = "currentPage";

	private static ThreadLocal requestHolder = new ThreadLocal();
	private static ThreadLocal dynamicParamsHolder = new ThreadLocal();
	private static ServletContext servletContext;

	/**
	 * 将request放进线程变量中
	 */
	public static final void setRequest(HttpServletRequest request) {
		requestHolder.set(request);
	}

	/**
	 * 从进线程变量中获取request
	 */
	public static final HttpServletRequest getRequest() {
		return (HttpServletRequest) requestHolder.get();
	}

	/**
	 * 进线程变量中获取动态参数(与request生命周期一致)
	 */
	public static final Map getDynamicParamsMap() {
		return (Map) dynamicParamsHolder.get();
	}

	/**
	 * 将动态参数放进线程变量中
	 */
	public static final void setDynamicParamsMap(Map dynamicParamsMap) {
		dynamicParamsHolder.set(dynamicParamsMap);
	}

	public static final ServletContext getServletContext() {
		return servletContext;
	}

	public static final void setServletContext(ServletContext servletContext) {
		WebContextHolderUtil.servletContext = servletContext;
	}

	public static void cleanRequestHolder() {
//		requestHolder.remove();
		requestHolder.set(null);
	}

	public static void cleanDynamicParamsHolder() {
//		dynamicParamsHolder.remove();
		dynamicParamsHolder.set(null);
	}
}
