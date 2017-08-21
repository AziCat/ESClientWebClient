package cn.sinobest.base.util;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * http相关工具类
 * @author yjh
 * @date 2017.08.12
 */
public class HttpUtil {
    /**
     * 获取请求体中的字符串
     */
    public static String getBodyData(HttpServletRequest request) throws IOException {
        request.setCharacterEncoding("utf-8");
        StringBuilder data = new StringBuilder();
        String line;
        BufferedReader reader = null;
        reader = request.getReader();
        while (null != (line = reader.readLine())){
            data.append(line);
        }
        reader.close();
        return data.toString();
    }
}
