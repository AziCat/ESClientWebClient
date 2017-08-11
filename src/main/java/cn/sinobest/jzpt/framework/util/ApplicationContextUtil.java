package cn.sinobest.jzpt.framework.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 *
 * @author tangqing
 * @date 2016年12月29日-上午10:14:33
 */
@Component
public class ApplicationContextUtil implements ApplicationContextAware {
	private static ApplicationContext applicationContext;
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		ApplicationContextUtil.applicationContext = applicationContext;
	}

	public static Object getBean(String name) {
		return applicationContext.getBean(name);
	}

	public static <T> T getBean(String name, Class<T> clazz) {
		return applicationContext.getBean(name, clazz);
	}

	public static <T> T getBeanIfNotExistReturnNull(String beanId, Class<T> clazz){
		if(applicationContext == null || !applicationContext.containsBean(beanId)) {
			return null;
		}
	    return getBean(beanId, clazz);
	}

	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}
}
