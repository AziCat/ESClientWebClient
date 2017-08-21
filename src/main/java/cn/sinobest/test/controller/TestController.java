package cn.sinobest.test.controller;

import cn.sinobest.test.service.ITestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping ( "/test" )
public class TestController {
    @Autowired
    ITestService testService;

    @RequestMapping("/test.action")
	public void test(Model model){
        testService.test();
        //System.out.println("123123123");
    }
    @RequestMapping("/fulldose.action")
    public void fulldose(Model model){
        testService.fulldose();
    }
}
