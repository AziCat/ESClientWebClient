package cn.sinobest.base.util;

import java.util.Properties;

/**
 * 用于读取配置文件内容的工具类
 * @author yjh
 * @date 2017.08.09
 */
public class PropertiesUtil {
    private static Properties pro = new Properties();

    /**
     * 根据配置文件名字和key获取内容
     * @param name  配置文件
     * @param key   标识
     * @return  内容
     */
    public static Object getAsObject(String name, String key){
        try {
            pro.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(name));
            return pro.get(key);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("读取配置文件出错。name:"+name+"--key:"+key);
        }
        return null;
    }

    /**
     * 根据配置文件名字和key获取内容
     * @param name  配置文件
     * @param key   标识
     * @return  内容
     */
    public static String getAsString(String name, String key){
        try {
            pro.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(name));
            return pro.getProperty(key);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("读取配置文件出错。name:"+name+"--key:"+key);
        }
        return null;
    }

    /**
     * 根据配置文件名字和key获取内容
     * @param name  配置文件
     * @param key   标识
     * @return  内容
     */
    public static int getAsInt(String name, String key){
        try {
            pro.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(name));
            return Integer.parseInt(pro.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("读取配置文件出错。name:"+name+"--key:"+key);
        }
        return 0;
    }


/**
     * 根据配置文件名字和key获取内容
     * @param name  配置文件
     * @param key   标识
     * @return  内容
     */
    public static boolean getAsBoolean(String name, String key){
        try {
            pro.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(name));
            return Boolean.parseBoolean(pro.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("读取配置文件出错。name:"+name+"--key:"+key);
        }
        return false;
    }

    /**
     * 根据配置文件名字和key获取内容
     * @param name  配置文件
     * @param key   标识
     * @return  内容
     */
    public static long getAsLong(String name, String key){
        try {
            pro.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(name));
            return Long.parseLong(pro.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("读取配置文件出错。name:"+name+"--key:"+key);
        }
        return 0;
    }
}
