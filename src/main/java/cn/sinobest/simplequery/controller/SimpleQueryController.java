package cn.sinobest.simplequery.controller;

import cn.sinobest.base.util.HttpUtil;
import cn.sinobest.base.util.JsonUtil;
import cn.sinobest.simplequery.domain.ResultDto;
import cn.sinobest.simplequery.domain.SimpleParam;
import cn.sinobest.simplequery.service.ISimpleQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * 通用查询控制器
 */
@Controller
@RequestMapping("/SimpleQuery")
public class SimpleQueryController {
    @Autowired
    private ISimpleQueryService simpleQueryService;

    @ResponseBody
    @RequestMapping("/query.action")
    public void getQueryResult(HttpServletRequest request, HttpServletResponse response) {
        PrintWriter out; //获取写入对象
        response.setCharacterEncoding("UTF-8"); //设置编码格式
        response.setContentType("text/html");   //设置数据格式
        ResultDto result = new ResultDto();
        try {
            String queryString = HttpUtil.getBodyData(request);
            try {
                SimpleParam simpleParam = JsonUtil.json2BeanWithNullCheck(queryString, SimpleParam.class, SimpleParam.getNotNullList());
                result = simpleQueryService.getQueryResult(simpleParam);
            } catch (Exception e) {
                e.printStackTrace();
                result.setSuccessful("0");
                result.setTotal(0);
                result.setMsg(e.getMessage());
                result.setSources(new ArrayList<>());
            }
            out = response.getWriter();
            out.print(JsonUtil.bean2Json(result)); //将json数据写入流中
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
