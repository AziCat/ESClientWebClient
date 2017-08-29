package cn.sinobest.simplequery.service.impl;

import cn.sinobest.base.util.PropertiesUtil;
import cn.sinobest.es.util.ElasticSearchManager;
import cn.sinobest.simplequery.domain.ResultDto;
import cn.sinobest.simplequery.domain.SeniorParam;
import cn.sinobest.simplequery.domain.SimpleParam;
import cn.sinobest.simplequery.domain.Source;
import cn.sinobest.simplequery.service.ISimpleQueryService;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通用查询服务实现类
 *
 * @author yjh
 * @date 2017.08.15
 */
@Service("SimpleQueryServiceImpl")
public class SimpleQueryServiceImpl implements ISimpleQueryService {
    //es保留字,因为\是消义符，所以放在数组最前
    private static String[] FILTER_SYMBOL = {"\\", "+", "-", "=", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "/"};
    //过滤符号
    private static String[] REPLACE_SYMBOL = {">", "<"};
    private static String IK_NAME = "ik_fields.properties";

    private static String ES_CONFIG = "es_config.properties";

    //ik分词器适配字段map
    private Map<String, List> ikMap = new HashMap<>();

    @Override
    public ResultDto getQueryResult(SimpleParam simpleParam,String flag) throws Exception {
        TransportClient client = ElasticSearchManager.getClient();

        SearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder(simpleParam, client);

        //构建查询条件
        String queryContent = buildQueryContent(simpleParam,client);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder = setSimpleBoolQueryBuilder(simpleParam, queryContent, queryBuilder);

        if("1".equals(flag)){
            //设置高级查询参数
            setSeniorParam(simpleParam, queryBuilder);
        }

        searchRequestBuilder.setQuery(queryBuilder);
        setFromAndSize(simpleParam, searchRequestBuilder);
        setOrder(simpleParam, searchRequestBuilder);
        //返回字段
        setReturnField(simpleParam, searchRequestBuilder);
        //设置高亮显示
        setHighLight(searchRequestBuilder);


        //执行查询
        SearchResponse response = searchRequestBuilder.get();
        System.out.println("查询成功");
        return buildResult(simpleParam, response);
    }

    /**
     * 构建返回结果
     * @param simpleParam 参数实体
     * @param response es响应
     * @return 结构化的返回结果
     */
    private ResultDto buildResult(SimpleParam simpleParam, SearchResponse response) {
        ResultDto result = new ResultDto();
        result.setSuccessful("1");//成功标志
        float maxScore = response.getHits().getMaxScore();
        if (!Float.isNaN(maxScore)) {
            result.setMax_score(response.getHits().getMaxScore());//最大相关度得分
        }
        result.setTotal(response.getHits().getTotalHits());//满足结果数
        List<Source> sourceList = new ArrayList<>();

        //高亮字段
        String highLight = simpleParam.getHighLight();
        List<String> highLightList = new ArrayList<>();

        if(null != highLight && !"".equals(highLight)){
            highLightList = Arrays.asList(highLight.split(","));
        }

        //设置耗时
        TimeValue took = response.getTook();
        result.setTook(took.toString());

        //遍历结果集
        for (SearchHit hit : response.getHits()) {
            //结果集
            Map<String,Object> resultMap = hit.getSource();

            if(highLightList.size() > 0){
                //获取对应的高亮域
                Map<String, HighlightField> hf = hit.getHighlightFields();
                Set<Map.Entry<String, HighlightField>> hfSet = hf.entrySet();

                for (Map.Entry<String, HighlightField> e : hfSet) {
                    String key = e.getKey();
                    HighlightField h = e.getValue();
                    Text[] titleTexts = h.fragments();
                    for (Text text : titleTexts) {
                        if(highLightList.contains(key)){
                            resultMap.put(key,text.toString());
                        }
                    }
                }
            }
            Source item = new Source();
            item.set_id(hit.getId());
            item.set_index(hit.getIndex());
            float score = hit.getScore();
            if (!Float.isNaN(score)) {
                item.set_score(score);
            }
            item.set_type(hit.getType());
            item.setItem(resultMap);
            sourceList.add(item);

        }
        result.setSources(sourceList);
        return result;
    }

    /**
     * 设置高级查询参数
     * @param simpleParam SimpleParam
     * @param queryBuilder BoolQueryBuilder
     */
    private void setSeniorParam(SimpleParam simpleParam, BoolQueryBuilder queryBuilder) {
        //强转为高级查询参数实体
        SeniorParam seniorParam = (SeniorParam) simpleParam;
        //must
        Map<String,String[]> must = seniorParam.getMust();
        if(null != must){
            must.forEach((key,value)->{
                System.out.println("must-->"+key+":"+Arrays.toString(value));
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                for(String item : value){
                    boolQueryBuilder.should(QueryBuilders.matchQuery(key,item));
                }
                queryBuilder.must(boolQueryBuilder);
            });
        }
        //should
        Map<String,String[]> should = seniorParam.getShould();
        if(null != should){
            should.forEach((key,value)->{
                System.out.println("should-->"+key+":"+Arrays.toString(value));
                for(String item : value){
                    queryBuilder.should(QueryBuilders.matchQuery(key,item));
                }
            });
        }
        //mustNot
        Map<String,String[]> mustNot = seniorParam.getMustNot();
        if(null != mustNot){
            mustNot.forEach((key,value)->{
                System.out.println("mustNot-->"+key+":"+Arrays.toString(value));
                for(String item : value){
                    queryBuilder.mustNot(QueryBuilders.matchQuery(key,item));
                }
            });
        }
    }

    /**
     * 通用设置
     * @param simpleParam SimpleParam
     * @param queryContent queryContent
     * @param queryBuilder BoolQueryBuilder
     * @return BoolQueryBuilder
     */
    private BoolQueryBuilder setSimpleBoolQueryBuilder(SimpleParam simpleParam, String queryContent, BoolQueryBuilder queryBuilder) {
        //设置部门控制
        queryBuilder = queryBuilder.must(QueryBuilders.wildcardQuery("DEPARTMENTCODE", simpleParam.getDeptCode()+"*"));
        //设置时间控制
        long st = simpleParam.getStartTime();
        long et = simpleParam.getEndTime();
        if (0 != st && 0 != et) {
            queryBuilder = queryBuilder.must(QueryBuilders.rangeQuery("LASTUPDATEDTIME").gt(st).lt(et));
        }
        //设置删除标志控制
        queryBuilder = queryBuilder.mustNot(QueryBuilders.multiMatchQuery("1","AJ_DELETEFLAG","JQ_DELETEFLAG","RY_DELETEFLAG","DW_DELETEFLAG","DELETEFLAG"));
        //设置查询正文
        queryBuilder = queryBuilder.must(QueryBuilders.queryStringQuery(queryContent));
        return queryBuilder;
    }

    /**
     * 高亮设置
     * @param searchRequestBuilder SearchRequestBuilder
     */
    private void setHighLight(SearchRequestBuilder searchRequestBuilder) {
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span class=\"es_Keyword\">");
        highlightBuilder.postTags("</span>");
        searchRequestBuilder.highlighter(highlightBuilder);
    }

    /**
     * 设置返回字段
     * @param simpleParam SimpleParam
     * @param searchRequestBuilder SearchRequestBuilder
     */
    private void setReturnField(SimpleParam simpleParam, SearchRequestBuilder searchRequestBuilder) {
        String field = simpleParam.getField();
        if (null != field && !"".equals(field)) {
            System.out.println("field-->" + field);
            String[] fieldArr = field.split(",");
            searchRequestBuilder.setFetchSource(fieldArr, new String[]{});
        } else {
            System.out.println("field-->default");
        }
    }

    /**
     * 设置排序
     * @param simpleParam SimpleParam
     * @param searchRequestBuilder SearchRequestBuilder
     * @throws Exception orderType 错误
     */
    private void setOrder(SimpleParam simpleParam, SearchRequestBuilder searchRequestBuilder) throws Exception {
        String order = simpleParam.getOrder();
        if (null != order && !"".equals(order)) {
            order = order.trim();
            System.out.println("order-->" + order);
            String orderType = simpleParam.getOrderType();
            if (null == orderType || "".equals(orderType)) {
                orderType = "DESC";//默认降序
                searchRequestBuilder.addSort(order, SortOrder.DESC);
            } else {
                orderType = orderType.toUpperCase();
                if ("DESC".equals(orderType)) {
                    searchRequestBuilder.addSort(order, SortOrder.DESC);
                } else if ("ASC".equals(orderType)) {
                    searchRequestBuilder.addSort(order, SortOrder.ASC);
                } else {
                    throw new Exception("orderType 错误！");
                }
            }
            System.out.println("orderType-->" + orderType);
        }
    }

    /**
     * 设置结果集起始位置与大小
     * @param simpleParam simpleParam
     * @param searchRequestBuilder SearchRequestBuilder
     */
    private void setFromAndSize(SimpleParam simpleParam, SearchRequestBuilder searchRequestBuilder) {
        int from = simpleParam.getFrom();
        System.out.println("from-->" + from);
        searchRequestBuilder.setFrom(from);

        int size = simpleParam.getSize() == 0 ? 10 : simpleParam.getSize();
        System.out.println("size-->" + size);
        searchRequestBuilder.setSize(size);
    }

    /**
     * 获取 SearchRequestBuilder
     * @param simpleParam 参数实体
     * @param client es连接实例
     * @return SearchRequestBuilder
     */
    private SearchRequestBuilder getSearchRequestBuilder(SimpleParam simpleParam, TransportClient client) {
        System.out.println("index-->" + simpleParam.getIndex());
        System.out.println("type-->" + simpleParam.getType());
        return client.prepareSearch(simpleParam.getIndex())
                .setTypes(simpleParam.getType())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
    }

    /*public static void main(String[] args) {
        String testStr = "a\\sdfa+dfasd+f/FFFFFFF&&(1+2)=3";
        for (String s : FILTER_SYMBOL) {
            if (testStr.contains(s)) {
                System.out.println(s);
                testStr = testStr.replace(s, "\\" + s);
                System.out.println(testStr);
            }
        }
        System.out.println(isContainChinese(testStr));
    }*/

    /**
     * 构建查询条件
     * 查询条件使用 es的 Query String Query
     * 文档：https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
     *
     * @param simpleParam 参数实例
     * @param client TransportClient实例
     * @return 查询体
     */
    private String buildQueryContent(SimpleParam simpleParam, TransportClient client) throws Exception {
        StringBuilder builder = new StringBuilder();


        //es保留字消义
        String queryStr = simpleParam.getQueryStr();
        for (String s : FILTER_SYMBOL) {
            if (queryStr.contains(s)) {
                queryStr = queryStr.replace(s, "\\" + s);
            }
        }
        //es不能消义的保留字过滤
        for (String s : REPLACE_SYMBOL) {
            if (queryStr.contains(s)) {
                queryStr = queryStr.replace(s, "");
            }
        }

        //获取查询类型AND或者OR
        String queryType = simpleParam.getQueryType();

        if (null == queryType || "".equals(queryType)) {
            queryType = "AND";
        } else {
            queryType = queryType.trim().toUpperCase();
            if (!"AND|OR".contains(queryType)) {
                throw new Exception("queryType错误！");
            }
        }
        //以空格切割查询内容
        String[] queryStrArr = queryStr.split(" ");
        if (queryStrArr.length > 0) {
            builder.append(" (");
            //拆分包含中文的参数和其它参数
            List<String> chineseParamsList = new ArrayList<>();
            List<String> otherParamsList = new ArrayList<>();

            for (String param : queryStrArr) {
                if (isContainChinese(param)) {
                    chineseParamsList.add(param);
                } else {
                    otherParamsList.add(param);
                }
            }

            if (otherParamsList.size() > 0) {
                builder.append(" (_all:");
                for (int i = 0; i < otherParamsList.size(); i++) {
                    String item = otherParamsList.get(i);
                    builder.append("*").append(item).append("*");
                    if (i != otherParamsList.size() - 1) {
                        builder.append(" ").append(queryType).append(" ");
                    }
                }
                builder.append(")");

                if (chineseParamsList.size() > 0) {
                    builder.append(" ").append(queryType).append(" ");
                }
            }

            if (chineseParamsList.size() > 0) {
                builder.append(" ( ");

                //中文参数预处理
                chineseParamsList = pretreatmentCNList(chineseParamsList,client,simpleParam);

                //根据索引和类型获取ik分词器适配字段
                String index = simpleParam.getIndex();
                String type = simpleParam.getType();
                List fields = ikMap.get(index + "." + type);
                if (null == fields) {
                    String fieldsStr = PropertiesUtil.getAsString(IK_NAME, index + "." + type);
                    fields = Arrays.asList(fieldsStr != null ? fieldsStr.split(",") : new String[0]);
                    ikMap.put(index + "." + type, fields);
                }

                //遍历含中文参数
                for (int i = 0; i < chineseParamsList.size(); i++) {
                    builder.append(" ( ");
                    //遍历字段
                    for (int j = 0; j < fields.size(); j++) {
                        builder.append(fields.get(j)).append(":").append("*").append(chineseParamsList.get(i)).append("*");
                        if (j != fields.size() - 1) {
                            builder.append(" OR ");
                        }
                    }
                    builder.append(" ) ");

                    if (i != chineseParamsList.size() - 1) {
                        builder.append(" ").append(queryType).append(" ");
                    }
                }

                builder.append(" ) ");
            }

            builder.append(" )");
        }
        System.out.println("查询体-->" + builder.toString());

        return builder.toString();
    }

    /**
     * 中文参数预处理
     * @param chineseParamsList 原始中文参数列表
     * @param client TransportClient实例
     * @param simpleParam 参数
     * @return 分词后的中文参数列表
     */
    private List<String> pretreatmentCNList(List<String> chineseParamsList, TransportClient client, SimpleParam simpleParam) {
        //判断开关
        if(PropertiesUtil.getAsBoolean(ES_CONFIG,"search.cn_pretreatment")){
            //分词器
            String analyzer = PropertiesUtil.getAsString(ES_CONFIG,"search.cn_analyzer");
            //构建分析请求
            AnalyzeRequestBuilder analyzeRequestBuilder = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE,
                    simpleParam.getIndex(),chineseParamsList.toArray(new String[]{}));
            analyzeRequestBuilder.setTokenizer(analyzer);

            //查询
            List<AnalyzeResponse.AnalyzeToken> ikTokenList = analyzeRequestBuilder.execute().actionGet().getTokens();

            //处理结果
            List<String> pretreatedCNList = new ArrayList<>();
            ikTokenList.forEach(ikToken -> {
                String item = ikToken.getTerm();
                if(!pretreatedCNList.contains(item)){
                    pretreatedCNList.add(item);
                }
            });
            System.out.println("中文参数预处理结果:"+Arrays.toString(pretreatedCNList.toArray()));
            return pretreatedCNList;
        }
        return chineseParamsList;
    }

    /**
     * 是否包含中文
     *
     * @param item 字符串
     * @return true or false
     */
    private boolean isContainChinese(String item) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(item);
        return m.find();
    }
}
