package cn.sinobest.base.util;

import com.google.gson.*;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.List;

/**
 * json相关工具类
 *
 * @author yjh
 * @date 2017.08.12
 */
public class JsonUtil {
    /**
     * 字符转Json
     *
     * @param jsonStr     json字符
     * @param clazz       类
     * @param notNullList 要校验非空的字段列表
     * @param <T>         泛型
     * @return 对象实例
     * @throws Exception 转换异常
     */
    public static <T> T json2BeanWithNullCheck(String jsonStr, Class<T> clazz, List<String> notNullList) throws Exception {
        StringBuilder errorMsg = new StringBuilder();
        Gson gson = new Gson();
        T obj = gson.fromJson(jsonStr, clazz);
        for (String field : notNullList) {
            PropertyDescriptor pd = new PropertyDescriptor(field, clazz);
            //获取字段的get方法
            Method m = pd.getReadMethod();
            //调用get方法
            Object value = m.invoke(obj);
            if (null == value || "".equals(value)) {
                errorMsg.append(field).append(" ");
            }
        }
        if (!"".equals(errorMsg.toString())) {
            throw new Exception("字段：" + errorMsg.toString() + "不能为空");
        }
        return obj;
    }

    /**
     * 对象转json字符
     *
     * @param obj
     * @return
     */
    public static String bean2Json(Object obj) {
        Gson gson = new Gson();
        String jsonStr = gson.toJson(obj);
        return jsonStr;
    }
}
