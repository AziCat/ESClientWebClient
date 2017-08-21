package cn.sinobest.simplequery.service;

import cn.sinobest.simplequery.domain.ResultDto;
import cn.sinobest.simplequery.domain.SimpleParam;

/**
 * 通用查询接口
 * @author yjh
 * @date 2017.08.15
 */
public interface ISimpleQueryService {
    ResultDto getQueryResult(SimpleParam simpleParam) throws Exception;
}
