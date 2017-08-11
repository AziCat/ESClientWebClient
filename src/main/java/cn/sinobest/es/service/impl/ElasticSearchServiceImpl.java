package cn.sinobest.es.service.impl;

import cn.sinobest.es.service.IElasticSearchService;
import cn.sinobest.es.util.ElasticSearchManager;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * es服务类实现
 * @author yjh
 * @date 2017.08.10
 */
@Service("ElasticSearchService")
public class ElasticSearchServiceImpl implements IElasticSearchService{


    public void sendBulkIndexRequest(Map<String, Map> dataMap, String index, String type) {
        //获取客户端
        TransportClient client = ElasticSearchManager.getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        //构建请求
        for(Map.Entry<String, Map> item : dataMap.entrySet()){
            bulkRequest.add(client.prepareIndex(index,type,item.getKey()).setSource(item.getValue()));
        }
        //发送请求
        BulkResponse bulkResponse = bulkRequest.get();
        //失败处理
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
    }

    public void sendBulkProcessRequest(Map<String, Map> dataMap, String index, String type) {
        BulkProcessor bulkProcessor = ElasticSearchManager.getBulkProcessor();
        //构建请求
        for(Map.Entry<String, Map> item : dataMap.entrySet()){
            bulkProcessor.add(new IndexRequest(index,type,item.getKey()).source(item.getValue()));
        }
        ElasticSearchManager.closeBulkProcess();
    }

}
