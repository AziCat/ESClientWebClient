package cn.sinobest.es.service;

import java.util.List;
import java.util.Map;

/**
 * es服务接口
 *
 * @author yjh
 * @date 2017.08.10
 */
public interface IElasticSearchService {
    /**
     * 发送批量请求
     *
     * @param dataMap 需要保存or更新的数据集，map的每个key作为每个文档的id，map的大小最好不要超过1000
     * @param index 索引名
     * @param type 类型
     */
    void sendBulkIndexRequest(Map<String, Map> dataMap, String index, String type);

    /**
     * 使用批量处理器发送请求
     *
     * @param dataMap 需要保存or更新的数据集，map的每个key作为每个文档的id
     * @param index 索引名
     * @param type 类型
     */
    void sendBulkProcessRequest(Map<String, Map> dataMap, String index, String type);

}
