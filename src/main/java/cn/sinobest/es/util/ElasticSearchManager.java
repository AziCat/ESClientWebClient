package cn.sinobest.es.util;

import cn.sinobest.base.util.PropertiesUtil;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * es管理工具
 * @author yjh
 * @date 2017.08.09
 */
public class ElasticSearchManager {
    private static TransportClient client;
    private static BulkProcessor bulkProcessor;
    private final static String PROPERTY_NAME = "es_config.properties";                      //配置文件名
    private final static String CLUSTER_NAME = "cluster.name";                               //集群
    private final static String ES_NODES = "es.nodes";                                       //节点列表
    private final static String TIMEOUT = "client.transport.ping_timeout";                   //超时
    private final static String CHECK_CONNECT = "client.transport.nodes_sampler_interval";   //检查连接
    private final static String SNIFF = "client.transport.sniff";                            //嗅探开关

    private final static String PROCESS_CONFIG = "bulk_processor.properties";                //批处理器配置文件名
    private final static String AWAIT_CLOSE = "await.close";                                 //批处理器延时关闭时间
    private final static String BULK_ACTIONS = "bulk.actions";                               //每多少条进行提交
    private final static String BULK_SIZE = "bulk.size";                                     //每多少M进行提交
    private final static String FLUSH_INTERVAL = "flush.interval";                           //每多少秒进行提交
    private final static String CONCURRENT_REQUESTS = "concurrent.requests";                 //并行请求数
    private final static String INITIAL_DELAY = "initial_delay";
    private final static String MAX_RETRIES_NUMBER = "max.retries.number";

    /**
     * 单例获取批处理器
     * @return es批处理器实例
     */
    public static BulkProcessor getBulkProcessor(){
        if(null == bulkProcessor){
            initBulkProcessor();
        }
        return bulkProcessor;
    }

    /**
     * 关闭批处理器
     */
    public static void closeBulkProcess() {
         //关闭资源
        try {
            long awaitClose = PropertiesUtil.getAsLong(PROCESS_CONFIG,AWAIT_CLOSE);
            //awaitClose秒后关闭处理器
            boolean isClose = bulkProcessor.awaitClose(awaitClose , TimeUnit.SECONDS);
            while (!isClose){
                isClose = bulkProcessor.awaitClose(awaitClose, TimeUnit.SECONDS);
            }
            bulkProcessor = null;
            System.out.println("关闭批处理器");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    private static void initBulkProcessor() {
        //构建处理器
        bulkProcessor = BulkProcessor.builder(
        getClient(),
        new BulkProcessor.Listener() {
            //处理前操作
            public void beforeBulk(long executionId,
                                   BulkRequest request) {  }
            //处理后操作
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  BulkResponse response) {  }
            //处理后失败操作
            public void afterBulk(long executionId,
                                  BulkRequest request,
                                  Throwable failure) {  }
        })
        .setBulkActions(PropertiesUtil.getAsInt(PROCESS_CONFIG,BULK_ACTIONS))//多少条提交一次
        .setBulkSize(new ByteSizeValue(PropertiesUtil.getAsInt(PROCESS_CONFIG, BULK_SIZE), ByteSizeUnit.MB))//多少M提交一次
        .setFlushInterval(TimeValue.timeValueSeconds(PropertiesUtil.getAsInt(PROCESS_CONFIG,FLUSH_INTERVAL)))//多少秒提交一次
        .setConcurrentRequests(PropertiesUtil.getAsInt(PROCESS_CONFIG,CONCURRENT_REQUESTS))//并行请求数
        .setBackoffPolicy(
            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(PropertiesUtil.getAsInt(PROCESS_CONFIG,INITIAL_DELAY))
                    ,PropertiesUtil.getAsInt(PROCESS_CONFIG,MAX_RETRIES_NUMBER)))
        .build();
    }
    /**
     * 单例获取es客户端
     * @return es客户端实例
     */
    public static TransportClient getClient(){
        if(null == client){
            try {
                initClient();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("创建es客户端异常。");
            }
        }
        return client;
    }

    /**
     * 初始化客户端
     */
    private static void initClient() throws Exception{
        //配置参数
        Settings settings = Settings.builder()
                .put(CLUSTER_NAME, PropertiesUtil.getAsObject(PROPERTY_NAME,CLUSTER_NAME))
                .put(TIMEOUT,PropertiesUtil.getAsObject(PROPERTY_NAME,TIMEOUT))
                .put(CHECK_CONNECT,PropertiesUtil.getAsObject(PROPERTY_NAME,CHECK_CONNECT))
                .put(SNIFF,PropertiesUtil.getAsObject(PROPERTY_NAME,SNIFF))
                .build();

        client = new PreBuiltTransportClient(settings);

        //添加节点
        String nodesStr = (String) PropertiesUtil.getAsObject(PROPERTY_NAME,ES_NODES);
        if(null != nodesStr){
            String[] nodesArr = nodesStr.split(",");
            for(String node : nodesArr){
                String ip = node.split(":")[0];                 //ip地址
                int port = Integer.parseInt(node.split(":")[1]);//端口
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip),port));
                System.out.println("添加节点：" + node);
            }
        }
    }
}
