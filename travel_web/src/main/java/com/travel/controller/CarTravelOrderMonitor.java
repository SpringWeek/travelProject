package com.travel.controller;

import org.springframework.boot.SpringApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@RestController
@RequestMapping("/home")
public class CarTravelOrderMonitor {

    /**
     * 项目首页
     *
     * @return
     */
    @RequestMapping("/index")
    public String home() {
        return "/index.html";
    }


    /**
     * 轨迹监控
     *
     * @return
     */
    @RequestMapping("/trackmonitor")
    public ModelAndView TrackMonitor() {
        return new ModelAndView("/trackmonitor.html");
    }

    /**
     * 出行迁途
     *
     * @return
     */
    @RequestMapping("/movingway")
    public ModelAndView MovingWay() {
        return new ModelAndView("/movingway.html");
    }

//    public static void main(String[] args) {
//        SpringApplication.run(CarTravelOrderMonitor.class,args);
//    }

}
