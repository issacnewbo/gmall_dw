package c.a.gmall.publisher.controller;


import c.a.gmall.publisher.bean.Option;
import c.a.gmall.publisher.bean.Stat;
import c.a.gmall.publisher.service.PublisherService;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

//    @GetMapping("realtime-total")
//    public String getRealtimeTotal(@RequestParam("date") String dateString) {
//        Long dauTotal = publisherService.getDauTotal(dateString);
//
//        List<Map<String, java.io.Serializable>> totalList = new ArrayList<Map<String, java.io.Serializable>>();
//        HashMap<String, java.io.Serializable> dauMap = new HashMap<String, java.io.Serializable>();
//
//        dauMap.put("id", "dau");
//        dauMap.put("name", "新增日活");
//        dauMap.put("value", dauTotal);
//
//        totalList.add(dauMap);
//
//
//        HashMap<String, java.io.Serializable> midMap = new HashMap<String, java.io.Serializable>();
//
//        midMap.put("id", "new_mid");
//        midMap.put("name", "新增设备");
//        midMap.put("value", 323);
//
//        totalList.add(midMap);
//
//        Map<String, java.io.Serializable> orderAmountMap = new HashMap<String, java.io.Serializable>();
//        orderAmountMap.put("id", "order_amount");
//        orderAmountMap.put("name", "新增交易额");
//        Double orderAmount = publisherService.getOrderAmount(dateString);
//        orderAmountMap.put("value", orderAmount);
//        totalList.add(orderAmountMap);
//
//        return JSON.toJSONString(totalList);
//
//    }
//
//    @GetMapping("realtime-hour")
//    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String dateString) {
//        if ("dau".equals(id)) {
//            Map<String, Long> dauTotalHoursTD = publisherService.getDauTotalHours(dateString);
//            String yesterday = getYesterday(dateString);
//            Map<String, Long> dauTotalHoursYD = publisherService.getDauTotalHours(yesterday);
//
//            Map<String, Map<String, Long>> hourMap = new HashMap<String, Map<String, Long>>(2);
//            hourMap.put("today", dauTotalHoursTD);
//            hourMap.put("yesterday", dauTotalHoursYD);
//
//            return JSON.toJSONString(hourMap);
//        } else if ("order_amount".equals(id)) {
//            Map<String, Map<String, Double>> hourMap = new HashMap<String, Map<String, Double>>(2);
//            Map<String, Double> orderHourTMap = publisherService.getOrderAmountHours(dateString);
//            String ydate = getYesterday(dateString);
//            Map<String, Double> orderHourYMap = publisherService.getOrderAmountHours(ydate);
//            hourMap.put("yesterday", orderHourYMap);
//            hourMap.put("today", orderHourTMap);
//            return JSON.toJSONString(hourMap);
//        }
//        return null;
//    }
//
//    private String getYesterday(String today) {
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
//
//        try {
//            Date todayD = simpleDateFormat.parse(today);
//            Date yesterdayD = DateUtils.addDays(todayD, -1);
//            String yesterday = simpleDateFormat.format(yesterdayD);
//            return yesterday;
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int startpage, @RequestParam("size") int size, @RequestParam("keyword") String keyword) {
        Map searchResultMap = publisherService.getSaleDetail(date, keyword, size, startpage);
        Long total = (Long) searchResultMap.get("total");
        List saleDetailList = (List) searchResultMap.get("list");
        Map ageMap = (Map) searchResultMap.get("ageMap");
        Map genderMap = (Map) searchResultMap.get("genderMap");

        // 年龄options
        long age_20Count = 0;
        long age20_30Count = 0;
        long age30_Count = 0;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Long age = Long.parseLong(entry.getKey().toString());
            Long count = Long.parseLong(entry.getValue().toString());
            if (age < 20) {
                age_20Count += count;
            } else if (age < 30) {
                age20_30Count += count;
            } else {
                age30_Count += count;
            }
        }
        Double age_20Ratio = Math.round(age_20Count * 1000L / total) / 10D;
        Double age20_30Ratio = Math.round(age20_30Count * 1000L / total) / 10D;
        Double age30_Ratio = Math.round(age30_Count * 1000L / total) / 10D;

        List<Option> ageList = new ArrayList<>();
        ageList.add(new Option("20岁以下", age_20Ratio));
        ageList.add(new Option("20岁到30岁", age20_30Ratio));
        ageList.add(new Option("30岁及以上", age30_Ratio));

        // 性别options
        Long femaleCount = Long.parseLong(genderMap.get("F").toString());
        Long maleCount = Long.parseLong(genderMap.get("M").toString());

        Double femaleRatio = Math.round(femaleCount * 1000L / total) / 10D;
        Double maleRatio = Math.round(maleCount * 1000L / total) / 10D;

        List<Option> genderList = new ArrayList<>();
        genderList.add(new Option("男", femaleRatio));
        genderList.add(new Option("女", maleRatio));

        // 完成stat
        ArrayList<Stat> statsList = new ArrayList<>();
        statsList.add(new Stat("用户年龄占比", ageList));
        statsList.add(new Stat("用户性别占比", genderList));

        // 做成结果
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("total", total);
        resultMap.put("stat", statsList);
        resultMap.put("detail", saleDetailList);

        return JSON.toJSONString(resultMap);
    }

}
