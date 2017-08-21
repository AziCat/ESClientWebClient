package cn.sinobest.simplequery.service.impl;

import cn.sinobest.base.util.PropertiesUtil;
import cn.sinobest.es.util.ElasticSearchManager;
import cn.sinobest.simplequery.domain.ResultDto;
import cn.sinobest.simplequery.domain.SimpleParam;
import cn.sinobest.simplequery.domain.Source;
import cn.sinobest.simplequery.service.ISimpleQueryService;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
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

    //ik分词器适配字段map
    private Map<String, List> ikMap = new HashMap<>();

    @Override
    public ResultDto getQueryResult(SimpleParam simpleParam) throws Exception {
        TransportClient client = ElasticSearchManager.getClient();
        //构建查询条件
        String queryContent = buildQueryContent(simpleParam);
        //构建查询体
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(queryContent);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(simpleParam.getIndex())
                .setTypes(simpleParam.getType())
                .setSearchType(SearchType.DEFAULT)
                .setQuery(queryBuilder);

        System.out.println("-----------------查询参数----------------");
        System.out.println("index-->" + simpleParam.getIndex());
        System.out.println("type-->" + simpleParam.getType());
        System.out.println("查询体-->" + queryContent);

        int from = simpleParam.getFrom();
        System.out.println("from-->" + from);
        searchRequestBuilder.setFrom(from);

        int size = simpleParam.getSize() == 0 ? 10 : simpleParam.getSize();
        System.out.println("size-->" + size);
        searchRequestBuilder.setSize(size);

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
                System.out.println("orderType-->" + orderType);
            }
        }
        //返回字段
        String field = simpleParam.getField();
        if (null != field && !"".equals(field)) {
            System.out.println("field-->" + field);
            String[] fieldArr = field.split(",");
            searchRequestBuilder.setFetchSource(fieldArr, new String[]{});
        } else {
            System.out.println("field-->default");
        }

        //设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span class=\"es_Keyword\">");
        highlightBuilder.postTags("</span>");
        searchRequestBuilder.highlighter(highlightBuilder);

        //执行查询
        SearchResponse response = searchRequestBuilder.get();
        System.out.println("查询成功");
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

        //遍历结果集
        for (SearchHit hit : response.getHits()) {
            //结果集
            Map resultMap = hit.getSource();

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
     * @return 查询体
     */
    private String buildQueryContent(SimpleParam simpleParam) throws Exception {
        StringBuilder builder = new StringBuilder();
        //部门控制
        builder.append(" (DEPARTMENTCODE:" + simpleParam.getDeptCode() + "*) AND ");
        //时间过滤条件
        long st = simpleParam.getStartTime();
        long et = simpleParam.getEndTime();
        if (0 != st && 0 != et) {
            builder.append(" (LASTUPDATEDTIME:[" + st + " TO " + et + "]) AND ");
        }


        //添加非删除过滤
        builder.append(" (-JQ_DELETEFLAG:1 AND -RY_DELETEFLAG:1 AND -DW_DELETEFLAG:1 AND -AJ_DELETEFLAG:1 AND -DELETEFLAG:1) ");

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
            builder.append(" AND (");
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

                //根据索引和类型获取ik分词器适配字段
                String index = simpleParam.getIndex();
                String type = simpleParam.getType();
                List fields = ikMap.get(index + "." + type);
                if (null == fields) {
                    String fieldsStr = PropertiesUtil.getAsString(IK_NAME, index + "." + type);
                    fields = Arrays.asList(fieldsStr.split(","));
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


        return builder.toString();
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
